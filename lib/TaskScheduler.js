"use strict";

var _und = require('underscore'),
    async = require('async'),
    util = require('util'),
    EventEmitter = require('events').EventEmitter,
    redis = require('redis'),
    debug = require('debug'),
    Errors = require('./Errors'),
    TaskDAO = require('./TaskDAO'),
    TaskUtils = require('./TaskUtils'),
    defaults = require('./defaults');


function _providedOrDefault(provided, key) {
    return (provided === undefined) ? defaults[key] : provided;
}

/**
 * @class TaskScheduler
 * Main polling mechanism and router for delegating Tasks to TaskRunners
 * Contains a single instance of the DAO which can be accessed directly
 *
 * @constructor
 * @param {Object} opts
 * @config {Object} redis - A config hash for redis.host, redis.port, redis.auth
 * @config {Object} redisClient - a Redis client for querying and manipulating tasks in the queue
 * @config {Object} redisClientSub - a separate Redis client to be used for the scheduler subscription
 * @config {Object} redisKeys - any custom Redis keys to be used
 * @config {number} pollIntervalLength - interval in milliseconds to poll the queue for tasks to run
 * @config {function} logger - a custom logger can be passed in here (must implement debug, info, warn, and error fxns).  pass in false for no logging (noop)
 */
function TaskScheduler(opts) {
    opts = opts || {};

    // Task runners (handlers for specific tasks) pushed to this array
    this.taskRunners = opts.taskRunners || {};

    // Redis subscription requires its own redisClient since it will be actively listening
    opts.redis = _providedOrDefault(opts.redis, 'redis');
    opts.redis.options = _providedOrDefault(opts.redis.options, 'redis.options');
    if(opts.redis.auth && opts.redis.auth !== '') opts.redis.options.auth_pass = opts.redis.auth;
    this.redisClient = opts.redisClient || redis.createClient(opts.redis.port, opts.redis.host, opts.redis.options);
    this.redisClientCreated = !opts.redisClient;
    this.redisClientSub = opts.redisClientSub || redis.createClient(opts.redis.port, opts.redis.host, opts.redis.options);
    this.redisClientSubCreated = !opts.redisClientSub;

    // Can pass in custom redis keys to prevent namespace issues
    this.redisKeys = _providedOrDefault(opts.redisKeys, 'redisKeys');

    // Can pass in a custom logger ( write to file, send email, etc)
    if(_und.isBoolean(opts.logger) && !opts.logger) opts.logger = TaskUtils.noopLogger;
    var logger = _providedOrDefault(opts.logger, 'logger');
    this.logger = logger;

    // debug logger
    this.debug = debug('punctual');
    this.debug.log = function() {
        logger.debug.apply(logger, arguments);
    };

    // Frequency of the poll (in milliseconds), do not accept 0
    this.pollIntervalLength = opts.pollIntervalLength || defaults.pollIntervalLength;

    // Begin subscription to this task scheduler channel in Redis
    this.redisPubSubEnabled = _providedOrDefault(opts.redisPubSubEnabled, 'redisPubSubEnabled');
    this.redisPubSubChannel = _providedOrDefault(opts.redisPubSubChannel, 'redisPubSubChannel');

    // Pass in opts & redisKeys to the taskDAO broker
    this.taskDAO = TaskDAO.create(_und.extend(opts, { redisKeys: this.redisKeys, Task: opts.Task }));

    // steps to take when stopping the Scheduler
    this._stopSteps = [];

    // indicates a successful start
    this._isStarted = false;

    EventEmitter.call(this);
}


util.inherits(TaskScheduler, EventEmitter);
module.exports = TaskScheduler;



/**
 * Return a new TaskScheduler instance (singleton)
 * @param {Object} opts - passed thru to scheduler
 */
TaskScheduler.create = function(opts, cb){
    var instance;

    if( this instanceof TaskScheduler ) instance = this;
    else instance = new TaskScheduler(opts);

    if(_und.isFunction(cb)) return cb(null, instance);
    else return instance;
};



/**
 * Destroys this TaskScheduler instance
 */
TaskScheduler.prototype.destroy = function(cb){
    var self = this;
    async.series([
        // #stop is safe to call even if not #_isStarted
        function(next) {
            self.stop(next);
        },
        // close our two Redis clients, but only if we created them
        function(next) {
            if (! self.redisClientCreated) {
                return next();
            }
            TaskUtils.closeRedisClient(self.redisClient, next);
        },
        function(next) {
            if (! self.redisClientSubCreated) {
                return next();
            }
            TaskUtils.closeRedisClient(self.redisClientSub, next);
        },
        // destroy our TaskDAO
        function(next) {
            self.taskDAO.destroy(next);
        },
        function(next) {
            self.logger.info("TaskScheduler:: destroyed");

            return next();
        },
    ], cb);
};



/**
 * Setup the Redis pattern subscribe for the scheduler, listening on a provided channel (defaults to 'scheduler').
 * This PUB-SUB is the main form of communication between the TaskScheduler and other servers.
 * When a message is received, route to an active TaskRunner instance (ex/ scheduler:pushNotificationTaskRunner)
 *  or internally to TaskScheduler instance methods (ex/ scheduler stop)
 * @param {String} channel (optional)
 * @param {function} cb(error) (optional)
 */
TaskScheduler.prototype._setupRedisSubscription = function(cb){
    var self = this;

    // the callback could be executed by multiple independent events;
    //   this prevents duplicate invocation
    var cbOnce = _und.once(cb);

    function onRedisErr(err){
        var redisError = new Errors.RedisError(err);
        self.logger.error("TaskScheduler:: Redis client error %s", redisError);
        self.emit('error', redisError);
        return cbOnce(redisError);
    }

    self.redisClient.on("error", onRedisErr);
    self._stopSteps.push(function() {
        // remove listeners upon stop
        self.redisClient.removeListener("error", onRedisErr);
    });

    function onRedisSubscribed(channel, count){
        self.logger.info("TaskScheduler:: Subscribed to Redis channel %s", channel);
        return cbOnce(null, channel, count);
    }

    function onRedisMessage(pattern, channel, msg){
        self.logger.info("TaskScheduler:: Received subscription message: %s for pattern %s on channel %s", msg, pattern, channel);

        // extract the job from the channel
        var runnerChannel = channel.split(':').length > 0 ? channel.split(':')[1] : null;

        if( runnerChannel ){
            if( _und.isUndefined( self.taskRunners[runnerChannel] )){
                self.logger.error("TaskScheduler:: no job channel active with name: %s", runnerChannel);

            } else {

                // Emit message to the task runner if one is specified inside the channel
                self.taskRunners[runnerChannel].emit( msg );
            }

        } else if( channel.slice(0, self.redisPubSubChannel.length) === self.redisPubSubChannel){

            // No targeted taskRunner channel in PUBLISH, emit message from scheduler
            self.emit(msg);
        }
    }

    /**
     * Optionally pattern subscribe for messages relating to this TaskScheduler or its TaskRunners
     * If a specific TaskRunner is included in the message channel, emit over the corresponding TaskRunner instance
     * Otherwise emit directly from this TaskScheduler instance
     * (ex/ PUBLISH scheduler myMessage would cause this TaskScheduler instance to emit myMessage)
     * (ex/ PUBLISH scheduler:myTaskRunner myMessage would cause a registered TaskRunner instance to emit myMessage (if one exists)
     */
    function subscribe(){
        self.redisClientSub.on("error", onRedisErr);
        self.redisClientSub.once("psubscribe", onRedisSubscribed);
        self.redisClientSub.on("pmessage", onRedisMessage);

        self.redisClientSub.psubscribe( self.redisPubSubChannel + '*' );

        self._stopSteps.push(function() {
            // remove listeners upon stop
            self.redisClientSub.removeListener("error", onRedisErr);
            self.redisClientSub.removeListener("psubscribe", onRedisSubscribed);
            self.redisClientSub.removeListener("pmessage", onRedisMessage);
        });
    }

    function onRedisReady(){
        if (self.redisPubSubEnabled){
            return subscribe();
        }
        return cbOnce(null);
    }

    if (self.redisClient.ready) {
        // Redis is already ready
        return onRedisReady();
    }

    // Listen for the Redis client 'ready' to signal this instance has now been set up
    self.redisClient.once('ready', onRedisReady);
    self._stopSteps.push(function() {
        // remove listeners upon stop
        self.redisClient.removeListener("ready", onRedisReady);
    });
};


/**
 * Setup the Redis client + subscription
 * Start the polling
 * @param {Object} [opts]
 */
TaskScheduler.prototype.start = function(opts, cb){
    var self = this;
    if( _und.isFunction(opts) ) { cb = opts; opts = {}; }

    this._setupRedisSubscription(function(err){
        if(err){
            var redisErr = new Errors.RedisError(err);
            self.logger.error(redisErr);
            if(cb) return cb(redisErr);
            else return false;
        }

        // Setup the polling interval
        var pollInterval = setInterval(function(){
            self._onPoll();
        }, self.pollIntervalLength);

        self._stopSteps.push(function() {
            // cancel polling upon stop
            clearInterval( pollInterval );
        });

        self.emit('started', opts);

        self.logger.info("TaskScheduler:: started");
        self._isStarted = true;

        if(cb) cb(null);
    });
};


/**
 * Stop the queue polling by clearing the poll interval
 * @param {Object} [opts]
 */
TaskScheduler.prototype.stop = function(opts, cb){
    if( _und.isFunction(opts) ) { cb = opts; opts = {}; }

    // take all the steps upon stop
    //   at the moment, the steps are all synchronous.
    //   adding these as independent 'stopped' event listeners is insufficient;
    //   they must be executed even when a start was only *partially* successful
    this._stopSteps.forEach(function(stepFn) {
        stepFn();
    });
    this._stopSteps = [];

    if (this._isStarted) {
        this.emit('stopped', opts );

        this.logger.info("TaskScheduler:: stopped");
        this._isStarted = false;
    }

    if(cb) cb(null);
};


/**
 * Get all scheduled tasks in the Redis queue (used for display and info purposes)
 * @param {Object} [opts]
 * @param {function} [callback(error, {Array} Tasks )]
 */
TaskScheduler.prototype.getAllScheduledTasks = function(opts, cb){
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }

    this.taskDAO.getAllTasks(opts, cb);
};



/**
 * Save a task via the taskDAO
 * @param {Object} taskObj
 * @param {function} [cb]
 */
TaskScheduler.prototype.scheduleTask = function(taskObj, cb){
    this.taskDAO.saveTask(taskObj, cb);
};


/**
 * Update a task in-place using its ID
 * @param {String} id - task identifier
 * @param {Object} newTask - task with updates/changes (formatted or unformatted)
 * @param {Object} [opts] - optional params hash
 * @param {function} [callback(error, {Object} updatedTaskObj)]
 */
TaskScheduler.prototype.updateTaskById = function(id, newTask, opts, cb){
    var self = this;
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }
    opts = opts || {};

    this.taskDAO.updateTask( id, newTask, opts, function(err){
        if(err){
            self.logger.error("TaskScheduler:: Error updating task:: " + util.inspect(err));
            if(cb) return cb(err);
            else return false;
        }
        self.emit('task:updated', { id: id, task: newTask });
        if(cb) cb(null, newTask);
    });
};


/**
 * Map a task fetched from the Redis queue to the appropriate TaskRunner (if one exists)
 * @param {Object} taskObj - formatted or unformatted
 * @param {function} [callback(error, {TaskRunner})]
 */
TaskScheduler.prototype.getRunnerForTask = function(taskObj, cb){

    /**
     *  Search through this.taskRunners for a TaskRunner instance with
     *  Task.type corresponding to TaskRunner.taskTypes
     */
    var runner = _und.filter( _und.values(this.taskRunners), function(taskRunnerInst){
        if( !_und.isArray(taskRunnerInst.taskTypes) && !_und.isString(taskRunnerInst.taskTypes) ) return false;
        if( taskRunnerInst.taskTypes.indexOf(taskObj.type) !== -1 ) return true;
        else return false;
    })[0];

    if(!runner) {
        if(cb) return cb("TaskScheduler:: Could not find an appropriate runner for task type: " + taskObj.type);
        else return false;
    }

    if(cb) cb(null, runner);
    else return runner;
};




/**
 * Fetch tasks to run from Redis based on the current timestamp and polling interval
 * @param {Object} [opts] - optional params hash
 * @param {function} callback(error, {Array} Tasks)
 */
TaskScheduler.prototype.getPolledTasks = function(opts, cb){
    var self = this;
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }
    opts = opts || {};

    var nowTime = new Date().getTime();
    //var pollLowerLimit = nowTime - self.pollIntervalLength;
    var pollLowerLimit = '-inf';

    this.taskDAO.getTasksInScheduleRange( pollLowerLimit, nowTime, function(err, scheduledTasks){
        if(err) {
            var pollErr = new Errors.PollingError("Error fetching tasks from poll range: " + util.inspect(err));
            self.logger.error(pollErr);
            return cb(pollErr);
        }

        // we'll get 0s all the time, and it's barely log-worthy
        var count = scheduledTasks.length;
        self.debug("TaskScheduler:: Found %s tasks to process", count);

        cb(null, scheduledTasks);
    });
};



/**
 * Map each task in an array to the appropriate TaskRunner and run each.
 * @param {Array} taskHashArr - array of tasks (formatted or unformatted)
 * @param {function} [callback(error, runtimeResponses)]
 */
TaskScheduler.prototype.runTasks = function(taskHashArr, cb){
    var self = this;
    var isAsync = (!!cb);

    if(!taskHashArr || (_und.isArray(taskHashArr) && taskHashArr.length === 0)){
        if(isAsync) return cb("TaskScheduler.runTasks called with empty taskHashArr");
        else return false;
    }

    function delegateToTaskRunner(taskObj, cb){
        if (! taskObj) {
            if(isAsync) cb();
            return;
        }

        var runner = self.getRunnerForTask(taskObj);
        if(!runner){
            var runnerErr = new Errors.InvalidTaskRunner("Could not find TaskRunner for taskObj "+ util.inspect(taskObj));
            self.logger.error(runnerErr);
            self.emit('task:unhandled', taskObj);
            if(isAsync) return cb(runnerErr);
            else return false;
        }

        if(isAsync) runner.runTask( taskObj, cb );
        else runner.runTask(taskObj);
    }

    if(isAsync) async.each(taskHashArr, delegateToTaskRunner, cb);
    else taskHashArr.forEach( delegateToTaskRunner );
};




/**
 * Callback from polling interval.  Get tasks within the poll range and run each.
 */
TaskScheduler.prototype._onPoll = function(cb){
    var self = this;

    this.debug("TaskScheduler:: Polling for tasks...");

    this.emit('poll', {});

    if(cb){

        // Fetch tasks and run in waterfall, passing the callback to this.runTasks
        var fns = [];
        fns.push( _und.bind(this.getPolledTasks, this, {} ) );
        fns.push( _und.bind(this.runTasks, this) );

        async.waterfall( fns, cb );

    } else {

        // Fire and forget
        this.getPolledTasks(function(err, tasks){
            if(err) return false;
            self.runTasks( tasks );
        });
    }
};


/**
 * Register a task runner function to handle specific task types
 * @param {function} runner - see TaskRunner class
 * @param {Array} taskTypes - task types to register for this runner class
 * @param {String} [runnerName] - key used in scheduler hash map
 * @param {function} [callback] - passed thru to TaskRunner.init method
 */
TaskScheduler.prototype.registerTaskRunner = function(runner, taskTypes, runnerName, cb){

    if(!runner || !_und.isFunction(runner.runTask)){
        this.logger.error(new Errors.InvalidTaskRunner("'runTask' is not a function" + util.inspect(runner)));
        return false;
    }

    // register task types with runner if explicitly provided
    if(_und.isString(taskTypes)) taskTypes = [taskTypes];
    if(_und.isArray(taskTypes)) runner.taskTypes = taskTypes;

    // get task runner name
    var mappedName = runnerName || runner.taskTypes[0] + runner.constructor.name;

    this.taskRunners[mappedName] = runner;

    // Call 'init' method on TaskRunner if it exists
    if( _und.isFunction( runner.init ) ) runner.init(cb);

    this.logger.info("TaskScheduler:: Success registering TaskRunner %s for task types %s", runnerName, runner.taskTypes);
};


/**
 * De-register a task runner function
 * @param {function|String} runner - see TaskRunner class
 * @return {String[]} taskRunners - array of updated taskRunners
 */
TaskScheduler.prototype.deregisterTaskRunner = function(runnerInst){
    var runnerName = runnerInst;

    // If the actual task runner instance provided, guess at the name based on type + constructor (see above)
    if(_und.isObject(runnerInst)) runnerName = runnerInst.taskTypes[0] + runnerInst.constructor.name;

    if(!this.taskRunners[runnerName]){
        this.logger.error(new Errors.InvalidTaskRunner("No TaskRunner found with name " + runnerName));
        return false;
    }

    // Call 'teardown' method on TaskRunner if it exists
    if( _und.isFunction(this.taskRunners[runnerName].teardown) ) this.taskRunners[runnerName].teardown();

    // Delete this instance from our mapping
    delete this.taskRunners[runnerName];

    this.logger.info("TaskScheduler:: Successfully deregistered task runner %s", runnerName);

};

