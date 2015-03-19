"use strict";

var _und = require('underscore'),
    redis = require('redis'),
    async = require('async'),
    Errors = require('./Errors'),
    Task = require('./Task'),
    TaskUtils = require('./TaskUtils'),
    defaults = require('./defaults');

/**
 * @class TaskDAO
 * A data-access module to abstract the Redis persistence
 *
 * @constructor
 * @param {Object} [opts]
 * @config {Object} [redis] - hash of redis config
 * @config {Object} [redisClient]
 * @config {Object} [redisKeys] - hash of redis keys to use for task-related stuff
 * @config {number} [redisTaskHashExpireAt]
 * @config {Task} [Task] - task model for validation & proper schema persistence
 */
function TaskDAO(opts){
    opts = opts || {};

    opts.redis = opts.redis || defaults.redis;
    opts.redis.options = opts.redis.options || defaults.redis.options;
    if(opts.redis.auth && opts.redis.auth !== '') opts.redis.options.auth_pass = opts.redis.auth;
    this.redisClient = opts.redisClient || redis.createClient(opts.redis.port, opts.redis.host, opts.redis.options);
    this.redisClientCreated = !opts.redisClient;
    this.redisKeys = opts.redisKeys || defaults.redisKeys;
    this.redisTaskHashExpireAt = opts.redisTaskHashExpireAt || defaults.redisTaskHashExpireAt;

    // Can pass a custom Task model for validation
    this.Task = opts.Task || Task;
}

module.exports = TaskDAO;



/**
 * Return a new TaskDAO instance
 * @param {Object} opts
 */
TaskDAO.create = TaskDAO.newInstance = function(opts){
    if( (this instanceof TaskDAO) ) return this;
    else return new TaskDAO(opts);
};



/**
 * Destroys this TaskDAO instance
 */
TaskDAO.prototype.destroy = function(cb){
    if (! this.redisClientCreated) {
        return cb();
    }
    TaskUtils.closeRedisClient(this.redisClient, cb);
};



/**
 * Return an array of tasks with scheduledFor timestamp within the provided limits
 * @param {String|Number} lowerLimit
 * @param {String|Number} upperLimit
 * @param {Object} [opts] - optional params hash
 * @param {Boolean} [opts.keepTaskIds] - true to keep Tasks during fetch
 * @param {function} cb - callback({Error}, {Task[]})
 */
TaskDAO.prototype.getTasksInScheduleRange = function(lowerLimit, upperLimit, opts, cb){
    var self = this;
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }
    opts = opts || {};

    // NOTE: -inf & +inf can also be passed here, so make sure limits are numbers before rounding
    lowerLimit = _und.isNumber(lowerLimit) ? Math.floor(lowerLimit) : lowerLimit;
    upperLimit = _und.isNumber(upperLimit) ? Math.floor(upperLimit) : upperLimit;

    var multiZset = this.redisClient.multi();

    // Pull from zset for IDs based on timestamp range
    multiZset.zrangebyscore([ this.redisKeys.scheduledJobsZset, lowerLimit, upperLimit ]);

    if(!opts || !opts.keepTaskIds) {
        // Atomically remove from Redis any members we have just fetched
        multiZset.zremrangebyscore([ this.redisKeys.scheduledJobsZset, lowerLimit, upperLimit ]);
    }

    multiZset.exec(function(err, multiZsetRes){
        if(err) return cb(new Errors.RedisError(err));

        // The zrangebyscore response are taskIds we have fetched then removed
        var taskIds = multiZsetRes[0];

        if( !taskIds ) return cb(null, []);

        var multiHget = self.redisClient.multi();
        for(var i = 0; i < taskIds.length; i++){
            multiHget.hgetall( self.redisKeys.scheduledJobsHash( taskIds[i] ) );
        }

        multiHget.exec(function(err, multiHgetRes){
            if(err) return cb(new Errors.RedisError(err));

            var taskObjs = _und.map(multiHgetRes, function(taskHash){
                if (! taskHash) return;
                var taskObj = self.Task.create(taskHash);
                if( taskObj instanceof self.Task ) return taskObj;
            });

            cb(null, taskObjs);
        });
    });
};


/**
 * Return an array of tasks with index-based lower & upper limit (ex/ limit + offset pagination)
 * @param {String | Number} lowerLimit
 * @param {String | Number} upperLimit
 * @param {Object} [opts] - optional params hash
 * @param {Boolean} [opts.keepTaskIds] - true to keep Tasks during fetch
 * @param {function} cb - callback(error, taskObjects)
 */
TaskDAO.prototype.getTasksInIndexRange = function(lowerLimit, upperLimit, opts, cb){
    var self = this;
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }
    opts = opts || {};

    // NOTE: -inf & +inf can also be passed here, so make sure limits are numbers before rounding
    lowerLimit = _und.isNumber(lowerLimit) ? Math.floor(lowerLimit) : lowerLimit;
    upperLimit = _und.isNumber(upperLimit) ? Math.floor(upperLimit) : upperLimit;

    var multiZset = this.redisClient.multi();

    // Pull from zset for IDs based on timestamp range
    multiZset.zrange([ this.redisKeys.scheduledJobsZset, lowerLimit, upperLimit ]);

    if(!opts || !opts.keepTaskIds) {
        // Atomically remove from Redis any members we have just fetched
        multiZset.zremrangebyrank([ this.redisKeys.scheduledJobsZset, lowerLimit, upperLimit ]);
    }

    multiZset.exec(function(err, multiZsetRes){
        if(err) return cb(new Errors.RedisError(err));

        // The zrangebyscore response are taskIds we have fetched then removed
        var taskIds = multiZsetRes[0];

        if( !taskIds ) return cb(null, []);

        var multiHget = self.redisClient.multi();
        for(var i = 0; i < taskIds.length; i++){
            multiHget.hgetall( self.redisKeys.scheduledJobsHash( taskIds[i] ) );
        }

        multiHget.exec(function(err, multiHgetRes){
            if(err) return cb(new Errors.RedisError(err));

            var taskObjs = _und.map(multiHgetRes, function(taskHash){
                if (! taskHash) return;
                var taskObj = self.Task.create(taskHash);
                if( taskObj instanceof self.Task ) return taskObj;
            });

            cb(null, taskObjs);
        });
    });
};


/**
 * Return an array of all tasks in the queue
 * @param {Object} [opts]
 * @param {function} cb - callback({Error}, Task[])
 */
TaskDAO.prototype.getAllTasks = function(opts, cb){
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }
    opts = opts || {};
    this.getTasksInScheduleRange('-inf', '+inf', opts, cb);
};



/**
 * Get info about a specific task by its ID
 * @param {String} id
 * @param {function} cb({Error}, {Task})
 */
TaskDAO.prototype.getTaskById = function(id, cb){
    var self = this;
    this.redisClient.hgetall( self.redisKeys.scheduledJobsHash( id ), function(err, taskHash){
        if(err) return cb(new Errors.RedisError(err));

        if(!taskHash) return cb(new Errors.InvalidTask("Could not find task using id " + id) );

        var taskObj = self.Task.create( taskHash );
        if( !(taskObj instanceof self.Task) ) return cb(new Errors.InvalidTask(taskHash));

        cb(null, taskObj);
    });
};


/**
 * Remove all tasks with scheduledFor timestamps within provided range
 * @param {String | Number} lowerLimit
 * @param {String | Number} upperLimit
 * @param {function} cb({Error}, {Task[]})
 */
TaskDAO.prototype.removeTaskMembersInRange = function(lowerLimit, upperLimit, cb){

    // NOTE: -inf & +inf can also be passed here, so make sure limits are numbers before rounding
    lowerLimit = _und.isNumber(lowerLimit) ? Math.floor(lowerLimit) : lowerLimit;
    upperLimit = _und.isNumber(upperLimit) ? Math.floor(upperLimit) : upperLimit;

    this.redisClient.zremrangebyscore([ this.redisKeys.scheduledJobsZset, lowerLimit, upperLimit ], function(err, res){
        if(err){
            if(cb) return cb(new Errors.RedisError(err));
            else return false;
        }
        if(cb) return cb(null, res);
    });
};



/**
 * Update a scheduled task in-place, fetching by its ID
 * @param {String} id
 * @param {Object} newTask - with updates to current task (formatted or unformatted)
 * @param {function} cb({Error}, updatedTaskObject) (optional)
 */
TaskDAO.prototype.updateTaskById = function(id, newTask, cb){
    // scheduling in milliseconds, expiry is in seconds
    var expireAtTimeSec = Math.floor(newTask.scheduledFor / 1000) + this.redisTaskHashExpireAt;
    var taskHashKey = this.redisKeys.scheduledJobsHash( id );

    this.redisClient.multi()
        .zadd([ this.redisKeys.scheduledJobsZset, newTask.scheduledFor, id ])
        .del([ taskHashKey ])
        .hmset( taskHashKey, newTask )
        .expireat( taskHashKey, expireAtTimeSec )
        .exec(function(err){
            if(err){
                if(cb) return cb(new Errors.RedisError(err));
                else return false;
            }
            if(cb) cb(null, newTask);
        });
};



/**
 * Save a task to the Redis queue
 * @param {Object | Task} task
 * @param {Function} cb({Error}, newTaskObject) (optional)
 */
TaskDAO.prototype.saveTask = function(task, cb){
    var taskObj = task;

    // If task is not a Task instance, apply schema + validations
    if(task && !(task instanceof this.Task)) taskObj = this.Task.create(task);

    // If the instance we just created fails validation
    if(!(taskObj instanceof this.Task)){
        if(cb) return cb(new Errors.InvalidTask(taskObj));
        else return false;
    }

    this.updateTaskById( taskObj.id, taskObj, cb);
};



/**
 * Save all tasks in an array to the Redis queue
 * @param {Task[]} tasks
 * @param {Function} cb
 * @callback [cb]
 */
TaskDAO.prototype.saveTasksMulti = function(tasks, cb){
    var self = this;

    if(cb){
        var fns = [];
        tasks.forEach(function(task){
            fns.push( _und.bind(self.updateTaskById, self, task.id, task) );
        });

        async.series( fns, cb );
    } else {
        tasks.forEach(function(task){
            self.updateTaskById( task.id, task );
        });
    }
};




/**
 * Remove a task from the Redis queue (ZSET and HASH)
 * @param {String} id
 * @param {function} [cb] - callback(error)
 */
TaskDAO.prototype.removeTaskById = function(id, cb){
    var multi = this.redisClient.multi();
    multi.zrem([ this.redisKeys.scheduledJobsZset, id ] );
    multi.del( this.redisKeys.scheduledJobsHash( id ) );

    if(cb) multi.exec(cb);
    else multi.exec();
};


/**
 * Remove multiple tasks from the Redis queue at once (ZSET and hashes)
 * @param {Task[] | string[]} ids
 * @param {function} [cb] - callback({Error})
 */
TaskDAO.prototype.removeTasksMulti = function(ids, cb){
    var self = this;

    if(cb){
        var fns = [];
        ids.forEach(function(id){
            if(_und.isObject(id)) id = id.id;
            fns.push( _und.bind( self.removeTaskById, self, id ) );
        });
        async.series( fns, cb );
    } else {
        ids.forEach(function(id){
            if(!_und.isObject(id)) id = id.id;
            self.removeTaskById(id);
        });
    }
};


/**
 * Remove all tasks from the Redis queue (ZSET and hashes)
 * @param {function} [cb] - callback({Error})
 */
TaskDAO.prototype.removeAllTasks = function(cb){
    var self = this;

    // Atomically fetch all tasks and delete from Redis (so there is no run conflict)
    this.redisClient.multi()
        .zrangebyscore([ this.redisKeys.scheduledJobsZset, '-inf', '+inf' ])
        .del( this.redisKeys.scheduledJobsZset )
        .exec(function(err, multiZsetRes){
            if(err) {
                if(cb) return cb(new Errors.RedisError(err));
                else return false;
            }

            var taskIds = multiZsetRes[0];

            if( !taskIds || taskIds.length === 0){
                if(cb) return cb(null, []);
                else return;
            }

            var multiDel = self.redisClient.multi();
            for(var i = 0; i < taskIds.length; i++){
                multiDel.del( self.redisKeys.scheduledJobsHash( taskIds[i] ) );
            }

            multiDel.exec(function(err){
                if(err){
                    if(cb) return cb(new Errors.RedisError(err));
                    else return false;
                }
                if(cb) cb(null);
            });
        });
};

