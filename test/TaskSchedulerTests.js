"use strict";

exports.name = "TaskSchedulerTests";

var assert = require('assert'),
    sinon = require('sinon'),
    TestHelpers = require('./helpers/TestHelpers'),
    TaskScheduler = require('../lib/TaskScheduler'),
    Task = require('../lib/Task'),
    TaskUtils = require('../lib/TaskUtils'),
    TaskRunner = require('../lib/TaskRunner'),
    TaskDAO = require('../lib/TaskDAO'),
    defaults = require('../lib/defaults'),
    Errors = require('../lib/Errors'),
    taskFixtures = require('./fixtures/tasks'),
    async = require('async'),
    _und = require('underscore');

var redisClient;
var scheduler;

describe( exports.name, function(){
    var sandbox;

    before(function(done){
        sandbox = sinon.sandbox.create();
        redisClient = TestHelpers.newRedisClient();

        done();
    });

    beforeEach(function(done){
        scheduler = TaskScheduler.create({
            taskRunners: { },
            redisClient: TestHelpers.newRedisClient(),
            redisClientSub: TestHelpers.newRedisClient()
        });

        scheduler.on('error', function(err){
            throw(err);
        });

        TestHelpers.assertTaskCount(scheduler.taskDAO, 0, done);
    });

    afterEach(function(done) {
        sandbox.restore();

        TestHelpers.clearTasks(scheduler.taskDAO, function(err) {
            assert.ifError(err);
            scheduler.stop(done);
        });
    });

    describe('#create()', function(){
        it('creates a new scheduler instance with no options', function(){
            var tmpScheduler = TaskScheduler.create();
            assert( tmpScheduler instanceof TaskScheduler );

            assert.deepEqual(tmpScheduler.taskRunners, {});

            assert(tmpScheduler.redisClient);
            assert.strictEqual(tmpScheduler.redisClient.port, defaults.redis.port);
            assert.strictEqual(tmpScheduler.redisClient.host, defaults.redis.host);
            assert(tmpScheduler.redisClient.options);
            assert(tmpScheduler.redisClientCreated);

            assert(tmpScheduler.redisClientSub);
            assert.strictEqual(tmpScheduler.redisClientSub.port, defaults.redis.port);
            assert.strictEqual(tmpScheduler.redisClientSub.host, defaults.redis.host);
            assert(tmpScheduler.redisClientSub.options);
            assert(tmpScheduler.redisClientSubCreated);

            assert.strictEqual(tmpScheduler.redisKeys, defaults.redisKeys);
            assert.strictEqual(tmpScheduler.logger, defaults.logger);
            assert.strictEqual(tmpScheduler.pollIntervalLength, defaults.pollIntervalLength);
            assert.strictEqual(tmpScheduler.redisPubSubEnabled, defaults.redisPubSubEnabled);
            assert.strictEqual(tmpScheduler.redisPubSubChannel, defaults.redisPubSubChannel);
        });

        it('creates a new scheduler instance with redis options', function(){
            var config = {
                redis: {
                    host: '127.0.0.1',
                    port: 6379, // not a good test!
                    options: { socket_nodelay: false },
                    auth: 'test',
                }
            };

            var tmpScheduler = TaskScheduler.create(config);
            assert( tmpScheduler instanceof TaskScheduler );

            assert(tmpScheduler.redisClient);
            assert.strictEqual(tmpScheduler.redisClient.port, config.redis.port);
            assert.strictEqual(tmpScheduler.redisClient.host, config.redis.host);
            assert.strictEqual(tmpScheduler.redisClient.options.socket_nodelay, config.redis.options.socket_nodelay);
            assert.strictEqual(tmpScheduler.redisClient.options.auth_pass, config.redis.auth);
            assert(tmpScheduler.redisClientCreated);

            assert(tmpScheduler.redisClientSub);
            assert.strictEqual(tmpScheduler.redisClientSub.port, config.redis.port);
            assert.strictEqual(tmpScheduler.redisClientSub.host, config.redis.host);
            assert.strictEqual(tmpScheduler.redisClientSub.options.socket_nodelay, config.redis.options.socket_nodelay);
            assert.strictEqual(tmpScheduler.redisClientSub.options.auth_pass, config.redis.auth);
            assert(tmpScheduler.redisClientSubCreated);
        });

        it('creates a new scheduler instance with options', function(){
            var redisClient = TestHelpers.newRedisClient();
            var config = { // all of these are unique token Objects!
                taskRunners: {},
                redisClient: redisClient,
                redisClientSub: redisClient,
                logger: false,
                pollIntervalLength: 0,
                redisPubSubEnabled: false,
                redisPubSubChannel: 0,
            };
            var tmpScheduler = TaskScheduler.create(config);

            assert( tmpScheduler instanceof TaskScheduler );

            assert.strictEqual(tmpScheduler.taskRunners, config.taskRunners);

            assert.strictEqual(tmpScheduler.redisClient, config.redisClient);
            assert(! tmpScheduler.redisClientCreated);

            assert.strictEqual(tmpScheduler.redisClientSub, config.redisClientSub);
            assert(! tmpScheduler.redisClientSubCreated);

            assert.strictEqual(tmpScheduler.redisKeys, config.redisKeys);
            assert.strictEqual(tmpScheduler.logger, TaskUtils.noopLogger);
            assert.strictEqual(tmpScheduler.pollIntervalLength, defaults.pollIntervalLength);
            assert.strictEqual(tmpScheduler.redisPubSubEnabled, config.redisPubSubEnabled);
            assert.strictEqual(tmpScheduler.redisPubSubChannel, config.redisPubSubChannel);
        });
    });

    describe('#destroy()', function(){

        it('calls #stop', function(done) {
            sandbox.stub(scheduler, 'stop', function() {
                // the callback
                arguments[arguments.length - 1]();
            });

            // even if it hasn't started
            assert.strictEqual(scheduler._isStarted, false);
            scheduler.destroy(function(err) {
                assert.ifError(err);
                assert(scheduler.stop.calledOnce);

                done();
            });
        });

        it('leaves open any supplied Redis clients', function(done){
            var config = {
                taskRunners: { },
                redisClient: TestHelpers.newRedisClient(),
                redisClientSub: TestHelpers.newRedisClient()
            };
            var tmpScheduler = TaskScheduler.create(config);
            assert.equal(tmpScheduler.redisClient, config.redisClient);
            assert.equal(tmpScheduler.redisClientSub, config.redisClientSub);

            async.series([
                function(next) {
                    tmpScheduler.redisClient.once('connect', function(){
                        if(tmpScheduler.redisClientSub.connected) return next();
                    });
                    tmpScheduler.redisClientSub.once('connect', function(){
                        if(tmpScheduler.redisClient.connected) return next();
                    });
                },
                function(next) {
                    assert(tmpScheduler.redisClient.connected);
                    assert(tmpScheduler.redisClientSub.connected);

                    tmpScheduler.destroy(next);
                },
                function(next) {
                    assert(tmpScheduler.redisClient.connected);
                    assert(tmpScheduler.redisClientSub.connected);

                    next();
                },
            ], done);
        });

        it('closes any Redis clients that created', function(done){
            var config = {
                taskRunners: { },
            };
            var tmpScheduler = TaskScheduler.create(config);
            assert(tmpScheduler.redisClient);
            assert(tmpScheduler.redisClientSub);

            async.series([
                function(next) {
                    tmpScheduler.redisClient.once('connect', function(){
                        if(tmpScheduler.redisClientSub.connected) return next();
                    });
                    tmpScheduler.redisClientSub.once('connect', function(){
                        if(tmpScheduler.redisClient.connected) return next();
                    });
                },
                function(next) {
                    assert(tmpScheduler.redisClient.connected);
                    assert(tmpScheduler.redisClientSub.connected);

                    tmpScheduler.destroy(next);
                },
                function(next) {
                    assert(! tmpScheduler.redisClient.connected);
                    assert(! tmpScheduler.redisClientSub.connected);

                    next();
                },
            ], done);
        });

        it('destroys its TaskDAO instance', function(done){
            var config = {
                taskRunners: { },
            };
            var tmpScheduler = TaskScheduler.create(config);
            sinon.stub(tmpScheduler.taskDAO, 'destroy', TaskDAO.prototype.destroy);

            async.series([
                function(next) {
                    assert(! tmpScheduler.taskDAO.destroy.called);

                    tmpScheduler.destroy(next);
                },
                function(next) {
                    assert(tmpScheduler.taskDAO.destroy.calledOnce);

                    next();
                },
            ], done);
        });
    });

    describe('#start()', function(){
        beforeEach(function() {
            // will poll virtually immediately
            scheduler.pollIntervalLength = 1;
        });

        it('emits a start event', function(done){
            var listener = sinon.spy();
            scheduler.on('started', listener);

            scheduler.start(function(err) {
                assert.ifError(err);
                assert(listener.calledOnce);

                // it is started
                assert(scheduler._isStarted);

                done();
            });
        });

        it('starts polling', function(done){
            sandbox.stub(scheduler, '_onPoll');

            scheduler.start(function(err) {
                assert.ifError(err);

                // we will poll virtually immediately
                assert(! scheduler._onPoll.called);
                setTimeout(function() {
                    assert(scheduler._onPoll.called);

                    done();
                }, 10);
            });
        });

        it('waits for the redisClient to be ready before starting', function(done) {
            // *sigh*
            assert(scheduler.redisClient.ready);

            // fake it
            var wasReady = scheduler.redisClient.ready;
            scheduler.redisClient.ready = false;
            setTimeout(function() {
                scheduler.redisClient.emit('ready');
            });

            scheduler.start(function(err) {
                assert.ifError(err);

                // it's a one-shot
                assert.equal(scheduler.redisClient.listeners("ready").length, 0);
                scheduler.redisClient.ready = wasReady;

                done();
            });

            // it'll be listening
            assert.equal(scheduler.redisClient.listeners("ready").length, 1);
        });

        it('immediately starts if the redisClient is ready', function(done) {
            assert(scheduler.redisClient.ready);

            scheduler.start(function(err) {
                assert.ifError(err);

                done();
            });

            assert.equal(scheduler.redisClient.listeners("ready").length, 0);
        });

        it('passes through an error from #_setupRedisSubscription', function(done) {
            var listener = sinon.spy();
            scheduler.on('started', listener);

            sandbox.stub(scheduler, '_setupRedisSubscription', function(cb) {
                cb(new Error('BOOM'));
            });
            sandbox.stub(scheduler, '_onPoll');

            scheduler.start(function(err) {
                assert(err);
                assert(err.message.indexOf('BOOM') !== -1);

                // did not emit the started event
                assert(! listener.called);

                // is not started
                assert(! scheduler._isStarted);

                // we never start polling
                assert(! scheduler._onPoll.called);
                setTimeout(function() {
                    assert(! scheduler._onPoll.called);

                    done();
                }, 10);
            });
        });
    });

    describe('#stop()', function(){
        it('emits a stop event when the scheduler was started', function(done){
            var listener = sinon.spy();
            scheduler.on('stopped', listener);

            scheduler._isStarted = true;
            scheduler.stop(function(err) {
                assert.ifError(err);
                assert(listener.calledOnce);

                // call it again after being stopped
                assert.strictEqual(scheduler._isStarted, false);
                scheduler.stop(function(err) {
                    assert.ifError(err);
                    assert(listener.calledOnce);

                    done();
                });
            });
        });

        it('execute all stop steps', function(done){
            var step = sinon.spy();
            scheduler._stopSteps.push(step);

            // even when not started
            assert.strictEqual(scheduler._isStarted, false);
            scheduler.stop(function(err) {
                assert.ifError(err);
                assert(step.calledOnce);
                assert.equal(scheduler._stopSteps.length, 0);

                done();
            });
        });

        describe('after a #start', function() {
            beforeEach(function(done) {
                // will poll virtually immediately
                scheduler.pollIntervalLength = 1;

                // will subscribe
                scheduler.redisPubSubEnabled = true;

                scheduler.start(done);
            });

            it('removes listeners', function(done) {
                // consumed by 'once'
                assert.equal(scheduler.redisClient.listeners("ready").length, 0);
                assert.equal(scheduler.redisClientSub.listeners("psubscribe").length, 0);

                assert.equal(scheduler.redisClient.listeners("error").length, 1);
                assert.equal(scheduler.redisClientSub.listeners("error").length, 1);
                assert.equal(scheduler.redisClientSub.listeners("pmessage").length, 1);

                scheduler.stop(function(err) {
                    assert.ifError(err);

                    assert.equal(scheduler.redisClient.listeners("error").length, 0);
                    assert.equal(scheduler.redisClientSub.listeners("error").length, 0);
                    assert.equal(scheduler.redisClientSub.listeners("pmessage").length, 0);

                    done();
                });
            });

            it('will stop polling', function(done) {
                sandbox.stub(scheduler, '_onPoll');

                setTimeout(function() {
                    assert(scheduler._onPoll.called);
                    scheduler._onPoll.reset();

                    scheduler.stop(function(err) {
                        assert.ifError(err);

                        // it stops getting called
                        assert(! scheduler._onPoll.called);
                        setTimeout(function() {
                            assert(! scheduler._onPoll.called);

                            done();
                        }, 10);
                    });
                }, 10);
            });
        });
    });

    describe('#runTasks()', function(){
        describe('with a Node.js callback', function() {
            it('needs an Array argument', function(done) {
                scheduler.runTasks( null, function(err){
                    assert(err);
                    done();
                });
            });

            it('needs an Array with at least one Task', function(done) {
                scheduler.runTasks( [], function(err){
                    assert(err);
                    done();
                });
            });

            it('runs a Task through a TaskRunner', function(done) {
                sandbox.stub(TaskRunner.prototype, 'runTask').callsArg(1);
                sandbox.stub(scheduler, 'getRunnerForTask', function(taskObj, cb) {
                    assert(! cb); // not used here
                    assert.equal(taskObj.type, 'good');

                    return new TaskRunner();
                });

                scheduler.runTasks( [
                    new Task({ scheduledFor: 0, type: 'good' }),
                ], function(err) {
                    assert.ifError(err);

                    assert.equal(scheduler.getRunnerForTask.callCount, 1);
                    assert.equal(TaskRunner.prototype.runTask.callCount, 1);

                    done();
                });
            });

            it('needs a TaskRunner for each Task', function(done) {
                sandbox.stub(TaskRunner.prototype, 'runTask').callsArg(1);
                sandbox.stub(scheduler, 'getRunnerForTask', function(taskObj, cb) {
                    assert(! cb); // not used here

                    return (taskObj.type === 'bad') ? null : new TaskRunner();
                });

                var listener = sandbox.spy(function(taskObj) {
                    assert.equal(taskObj.type, 'bad');
                });
                scheduler.on('task:unhandled', listener);

                scheduler.runTasks( [
                    new Task({ scheduledFor: 0, type: 'good' }),
                    new Task({ scheduledFor: 0, type: 'bad' }),
                    new Task({ scheduledFor: 0, type: 'good' }),
                ], function(err) {
                    assert(err instanceof Errors.InvalidTaskRunner);

                    // 1 = succeed
                    // 2 = fail, halt
                    // 3 = (unreached)
                    assert.equal(scheduler.getRunnerForTask.callCount, 2);
                    assert.equal(TaskRunner.prototype.runTask.callCount, 1);
                    assert.equal(listener.callCount, 1);

                    done();
                });
            });

            it('does not fail on a false-y Task', function(done) {
                sandbox.stub(TaskRunner.prototype, 'runTask');
                sandbox.stub(scheduler, 'getRunnerForTask');

                scheduler.runTasks( [
                    null
                ], function(err) {
                    assert.ifError(err);

                    assert(! scheduler.getRunnerForTask.called);
                    assert(! TaskRunner.prototype.runTask.called);

                    done();
                });
            });
        });

        describe('synchronously', function() {
            it('needs an Array argument', function() {
                assert.strictEqual(scheduler.runTasks( null ), false);
            });

            it('needs an Array with at least one Task', function() {
                assert.strictEqual(scheduler.runTasks( [] ), false);
            });

            it('runs a Task through a TaskRunner', function() {
                sandbox.stub(TaskRunner.prototype, 'runTask');
                sandbox.stub(scheduler, 'getRunnerForTask', function(taskObj, cb) {
                    assert(! cb); // not used here
                    assert.equal(taskObj.type, 'good');

                    return new TaskRunner();
                });

                var result = scheduler.runTasks( [
                    new Task({ scheduledFor: 0, type: 'good' }),
                ]);

                assert.strictEqual(result, undefined);

                assert.equal(scheduler.getRunnerForTask.callCount, 1);
                assert.equal(TaskRunner.prototype.runTask.callCount, 1);
            });

            it('needs a TaskRunner for each Task', function() {
                sandbox.stub(TaskRunner.prototype, 'runTask');
                sandbox.stub(scheduler, 'getRunnerForTask', function(taskObj, cb) {
                    assert(! cb); // not used here

                    return (taskObj.type === 'bad') ? null : new TaskRunner();
                });

                var listener = sandbox.spy(function(taskObj) {
                    assert.equal(taskObj.type, 'bad');
                });
                scheduler.on('task:unhandled', listener);

                var result = scheduler.runTasks( [
                    new Task({ scheduledFor: 0, type: 'good' }),
                    new Task({ scheduledFor: 0, type: 'bad' }),
                    new Task({ scheduledFor: 0, type: 'good' }),
                ] );

                assert.strictEqual(result, undefined);

                // 1 = succeed
                // 2 = fail
                // 3 = succeed
                assert.equal(scheduler.getRunnerForTask.callCount, 3);
                assert.equal(TaskRunner.prototype.runTask.callCount, 2);
                assert.equal(listener.callCount, 1);
            });

            it('does not fail on a false-y Task', function() {
                sandbox.stub(TaskRunner.prototype, 'runTask');
                sandbox.stub(scheduler, 'getRunnerForTask');

                var result = scheduler.runTasks( [
                    null
                ]);

                assert.strictEqual(result, undefined);

                assert(! scheduler.getRunnerForTask.called);
                assert(! TaskRunner.prototype.runTask.called);
            });
        });
    });

    describe('#getPolledTasks', function(){
        it('can get an array of tasks to run from the polling range', function(done){

            scheduler.getPolledTasks(function(err, tasks){
                assert.ifError(err);
                assert.ok(_und.isArray(tasks));

                done();
            });
        });
    });

    describe('#getAllScheduledTasks', function() {
        beforeEach(function(done) {
            TestHelpers.saveTasks(scheduler.taskDAO, taskFixtures, done);
        });

        it('returns all scheduled Tasks', function(done) {
            scheduler.getAllScheduledTasks(function(err, tasks){
                assert.ifError(err);
                assert.ok( _und.isArray(tasks) );
                assert.equal(tasks.length, taskFixtures.length);

                // anything we got was purged
                TestHelpers.assertTaskCount(scheduler.taskDAO, 0, done);
            });
        });

        it('can be asked to keep the Tasks it gets', function(done){
            scheduler.getAllScheduledTasks({
                keepTaskIds: true
            }, function(err, tasks){
                assert.ifError(err);
                assert.ok( _und.isArray(tasks) );
                assert.equal(tasks.length, taskFixtures.length);

                // nothing was purged
                TestHelpers.assertTaskCount(scheduler.taskDAO, taskFixtures.length, done);
            });
        });
    });

    describe('#registerTaskRunner', function(){
        it('adds a task runner to internal task type mapping', function(done){

            var testTaskRunner = TaskRunner.create({ });
            scheduler.registerTaskRunner(testTaskRunner, ['testType']);

            assert.ok( !_und.isEmpty(scheduler.taskRunners) );
            done();

        });
    });

    describe('#deregisterTaskRunner', function(){
        it('removes an active task runner from task type mapping', function(done){

            var testTaskRunner = TaskRunner.create({ });
            scheduler.registerTaskRunner(testTaskRunner, ['testType']);
            scheduler.deregisterTaskRunner(testTaskRunner);

            assert.ok( _und.isEmpty(scheduler.taskRunners) );

            done();

        });

    });

    describe('#getRunnerForTask', function(){
        var TYPE = 'testType';
        beforeEach(function() {
            var testTaskRunner = TaskRunner.create({ });
            scheduler.registerTaskRunner(testTaskRunner, [ TYPE ]);
        });

        describe('with a Node.js callback', function() {
            it('finds an appropriate runner for a task type', function(done){
                var testTask = Task.create({ type: TYPE });

                scheduler.getRunnerForTask(testTask, function(err, runner){
                    assert.ifError(err);
                    assert.ok( runner instanceof TaskRunner );
                    assert.ok(_und.isArray(runner.taskTypes));
                    assert.equal( runner.taskTypes[0], TYPE );
                    done();
                });
            });

            it('fails to find an appropriate runner for a task type', function(done){
                var testTask = Task.create({ type: 'BOGUS' });

                scheduler.getRunnerForTask(testTask, function(err, runner){
                    assert(err);
                    assert(! runner);
                    done();
                });
            });
        });

        describe('synchronously', function() {
            it('finds an appropriate runner for a task type', function(){
                var testTask = Task.create({ type: TYPE });

                var runner = scheduler.getRunnerForTask(testTask);
                assert.ok( runner instanceof TaskRunner );
                assert.ok(_und.isArray(runner.taskTypes));
                assert.equal( runner.taskTypes[0], TYPE );
            });

            it('fails to find an appropriate runner for a task type', function(){
                var testTask = Task.create({ type: 'BOGUS' });

                var runner = scheduler.getRunnerForTask(testTask);
                assert.strictEqual(runner, false);
            });
        });
    });

    describe('#onPoll', function(){
        it('fetches and run tasks on poll tick', function(done){

            try {
                scheduler._onPoll();
                done();
            } catch(err){
                return done(err);
            }

        });
    });

});
