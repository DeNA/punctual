"use strict";

exports.name = "TaskDAOTests";

var assert = require('assert'),
    _und = require('underscore'),
    util = require('util'),
    async = require('async'),
    TestHelpers = require('./helpers/TestHelpers'),
    Task = require('../lib/Task'),
    TaskDAO = require('../lib/TaskDAO'),
    Errors = require('../lib/Errors'),
    taskFixtures = require('./fixtures/tasks');

var redisClient, taskDAO;
var NO_SUCH_TASK = 'NO_SUCH_TASK';

describe( exports.name, function(){
    before(function(done){
        redisClient = TestHelpers.newRedisClient();
        taskDAO = TaskDAO.create({
            redisClient: redisClient
        });

        TestHelpers.clearTasks(taskDAO, done);
    });

    beforeEach(function(done) {
        TestHelpers.assertTaskCount(taskDAO, 0, done);
    });
    afterEach(function(done) {
        TestHelpers.clearTasks(taskDAO, done);
    });

    describe('#create()', function(){
        it('creates a new scheduler instance with no options', function(done){
            var tmpTaskDAO = TaskDAO.create();
            assert.ok( tmpTaskDAO instanceof TaskDAO );
            done();
        });
    });

    describe('#destroy()', function(){

        it('leaves open any supplied Redis clients', function(done){
            var config = {
                redisClient: TestHelpers.newRedisClient(),
            };
            var tmpTaskDAO = TaskDAO.create(config);
            assert.equal(tmpTaskDAO.redisClient, config.redisClient);

            async.series([
                function(next) {
                    tmpTaskDAO.redisClient.once('connect', next);
                },
                function(next) {
                    assert(tmpTaskDAO.redisClient.connected);

                    tmpTaskDAO.destroy(next);
                },
                function(next) {
                    assert(tmpTaskDAO.redisClient.connected);

                    next();
                },
            ], done);
        });

        it('closes any Redis clients that created', function(done){
            var tmpTaskDAO = TaskDAO.create();
            assert(tmpTaskDAO.redisClient);

            async.series([
                function(next) {
                    tmpTaskDAO.redisClient.once('connect', next);
                },
                function(next) {
                    assert(tmpTaskDAO.redisClient.connected);

                    tmpTaskDAO.destroy(next);
                },
                function(next) {
                    assert(! tmpTaskDAO.redisClient.connected);

                    next();
                },
            ], done);
        });
    });

    describe('#saveTask()', function(){
        it('can save a task', function(done){
            var tmpTask = taskFixtures[0];
            taskDAO.saveTask( tmpTask, function(err, task){
                assert(task);
                assert.ifError(err);
                done();
            });
        });

        it('fail informatively when the Task is false-y', function(done){
            taskDAO.saveTask( null, function(err){
                assert(err instanceof Errors.InvalidTask);
                done();
            });
        });
    });

    describe('#saveTasksMulti()', function(){
        it('can save multiple tasks', function(done){

            taskDAO.saveTasksMulti( _und.without(taskFixtures, taskFixtures[0]), function(err, res){
                assert.ifError(err);
                assert(res);

                done();
            });
        });
    });

    describe('#getTaskById()', function(){
        it('can fetch a Task hash provided a correct id', function(done){
            var tmpTask = taskFixtures[0];
            taskDAO.getTaskById( tmpTask.id, function(err, task){
                assert.ifError(err);
                assert(task);
                done();
            });
        });

        it('fails if the Task hash does not exist', function(done){
            taskDAO.getTaskById( NO_SUCH_TASK, function(err, task){
                assert(err instanceof Errors.InvalidTask);
                done();
            });
        });
    });

    describe('#getTasksInScheduleRange()', function(){
        var firstTask;
        var lowerLimit = 0;
        var upperLimit;
        beforeEach(function(done) {
            // our range will find only the earliest Task
            firstTask = taskFixtures.sort(function(a, b) {
                return (a.scheduledFor - b.scheduledFor);
            })[0];
            upperLimit = firstTask.scheduledFor;

            TestHelpers.saveTasks(taskDAO, taskFixtures, done);
        });

        it('can fetch all Tasks (with hash details) in a specific schedule range', function(done){
            taskDAO.getTasksInScheduleRange(lowerLimit, upperLimit, function(err, tasks){
                assert.ifError(err);
                assert.ok( _und.isArray(tasks) );
                assert.equal(tasks.length, 1);
                assert.equal(tasks[0].id, firstTask.id);

                tasks.forEach(function(task){
                    assert.ok( task instanceof Task );
                    assert.ok( task.active );
                    assert.ok( lowerLimit < task.scheduledFor <= upperLimit );
                });

                // anything we got was purged
                TestHelpers.assertTaskCount(taskDAO, (taskFixtures.length - 1), done);
            });
        });

        it('will not allow the same Task to be fetched twice', function(done){
            var fetchedTasks;
            var fetchTasks = function(done) {
                taskDAO.getTasksInScheduleRange(lowerLimit, upperLimit, function(err, tasks){
                    assert.ifError(err);
                    fetchedTasks = fetchedTasks.concat(tasks);
                    done();
                });
            };

            // 5 tests of 5 parallel fetches is sufficient
            var i = 0;
            async.whilst(function() {
                return i++ < 5;
            }, function(next) {
                fetchedTasks = [];
                return async.parallel([
                    fetchTasks,
                    fetchTasks,
                    fetchTasks,
                    fetchTasks,
                    fetchTasks,
                ], function(err) {
                    assert.ifError(err);

                    assert.equal(fetchedTasks.length, 1);

                    TestHelpers.saveTasks(taskDAO, taskFixtures, next);
                });
            }, done);
        });

        it('can be asked to keep the Tasks it gets', function(done){
            taskDAO.getTasksInScheduleRange(lowerLimit, upperLimit, {
                keepTaskIds: true
            }, function(err, tasks){
                assert.ifError(err);
                assert.ok( _und.isArray(tasks) );
                assert.equal(tasks.length, 1);

                // nothing was purged
                TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, done);
            });
        });

        it('does not fail when the Task data is missing', function(done){
            async.waterfall([
                function(next) {
                    redisClient.del( taskDAO.redisKeys.scheduledJobsHash( firstTask.id ), next );
                },
                function(result, next) {
                    taskDAO.getTasksInScheduleRange(lowerLimit, upperLimit, next);
                },
                function(tasks, next) {
                    // missing Task(s) take up space in the Array, vs. being compacted out
                    assert.ok( _und.isArray(tasks) );
                    assert.equal(tasks.length, 1);
                    assert(! tasks[0]);

                    next();
                },
            ], done);
        });
    });

    describe('#getTasksInIndexRange()', function(){
        beforeEach(function(done) {
            TestHelpers.saveTasks(taskDAO, taskFixtures, done);
        });

        it('can fetch all tasks (with hash details) in a specific index range', function(done){
            // only the first Task
            taskDAO.getTasksInIndexRange(0, 0, function(err, tasks){
                assert.ifError(err);
                assert.ok( _und.isArray(tasks) );
                assert.equal(tasks.length, 1);

                tasks.forEach(function(task){
                    assert.ok( task instanceof Task );
                    assert.ok( task.active );
                });

                // anything we got was purged
                TestHelpers.assertTaskCount(taskDAO, (taskFixtures.length - 1), done);
            });
        });

        it('can be asked to keep the Tasks it gets', function(done){
            taskDAO.getTasksInIndexRange(0, 0, {
                keepTaskIds: true
            }, function(err, tasks){
                assert.ifError(err);
                assert.ok( _und.isArray(tasks) );
                assert.equal(tasks.length, 1);

                // nothing was purged
                TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, done);
            });
        });

        it('does not fail when the Task data is missing', function(done){
            var len = taskFixtures.length;

            async.waterfall([
                function(next) {
                    // deleting any one of the Tasks proves the point
                    redisClient.del( taskDAO.redisKeys.scheduledJobsHash( taskFixtures[0].id ), next );
                },
                function(result, next) {
                    // all the Tasks
                    taskDAO.getTasksInIndexRange(0, len, next);
                },
                function(tasks, next) {
                    // missing Task(s) take up space in the Array, vs. being compacted out
                    assert.ok( _und.isArray(tasks) );
                    assert.equal(tasks.length, len);
                    assert.equal(_und.compact(tasks).length, len - 1);

                    next();
                },
            ], done);
        });
    });

    describe('#getAllTasks()', function(){
        beforeEach(function(done) {
            TestHelpers.saveTasks(taskDAO, taskFixtures, done);
        });

        it('can fetch all scheduled tasks', function(done){

            taskDAO.getAllTasks( function(err, tasks){
                assert.ifError(err);

                if(err) console.error("ERROR GETTING ALL TASKS: " + util.inspect(err));

                assert.ok( _und.isArray(tasks) );
                assert.ok( tasks[0] instanceof Task );

                tasks.forEach(function(task){
                    assert.ok( _und.isNumber(task.scheduledFor) );
                    //assert.ok( task.scheduledFor );
                    assert.ok( task.active );
                });
                done();
            });
        });
    });

    describe('#updateTaskById()', function(){
        it('creates a Task that does not exist', function(done){
            var tmpTask = taskFixtures[0];

            async.waterfall([
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, 0, next);
                },
                function(next) {
                    taskDAO.updateTaskById( tmpTask.id, tmpTask, next);
                },
                function(res, next) {
                    assert.equal(res.id, tmpTask.id);

                    TestHelpers.assertTaskCount(taskDAO, 1, next);
                },
            ], done);
        });

        it('can update a Task in place provided a valid id', function(done){
            var oldTask = taskFixtures[0];
            var newTask = _und.clone(oldTask);
            newTask.data = { updated: true };

            async.waterfall([
                function(next) {
                    TestHelpers.saveTasks(taskDAO, taskFixtures, next);
                },
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, next);
                },
                function(next) {
                    taskDAO.updateTaskById( oldTask.id, newTask, next);
                },
                function(res, next) {
                    assert.equal(res.id, oldTask.id);
                    assert(newTask.data.updated);

                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, next);
                },
            ], done);
        });

        it('can update a Task\'s id', function(done){
            var oldTask = taskFixtures[0];
            var newTask = _und.last(taskFixtures);

            async.waterfall([
                function(next) {
                    TestHelpers.saveTasks(taskDAO, taskFixtures, next);
                },
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, next);
                },
                function(next) {
                    taskDAO.updateTaskById( oldTask.id, newTask, next);
                },
                function(res, next) {
                    assert.equal(res.id, newTask.id);

                    // as a result, the same Task has been scheduled twice
                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, next);
                },
                function(next) {
                    taskDAO.getTasksInIndexRange(0, taskFixtures.length, next);
                },
                function(tasks, next) {
                    assert.equal(tasks.length, taskFixtures.length);
                    assert.equal(tasks[0].id, newTask.id);
                    assert.equal(tasks[1].id, newTask.id);

                    next();
                },
            ], done);
        });

        it('schedules the Task hash for auto-expiration', function(done){
            var gotHere;
            var tmpTask = _und.clone(taskFixtures[0]);
            tmpTask.scheduledFor = Date.now();

            // 2-second lifespan required, due to 1-second precision
            var redisTaskHashExpireAt = taskDAO.redisTaskHashExpireAt;
            taskDAO.redisTaskHashExpireAt = 2;

            async.waterfall([
                function(next) {
                    taskDAO.updateTaskById( tmpTask.id, tmpTask, next);
                },
                function(res, next) {
                    console.log('(still there in less than 1 second)');
                    setTimeout(next, 900);
                },
                function(next) {
                    taskDAO.getTaskById( tmpTask.id, next);
                },
                function(res, next) {
                    assert.equal(res.id, tmpTask.id);

                    console.log('(expired after 2 seconds)');
                    setTimeout(next, 1200);
                },
                function(next) {
                    gotHere = true;
                    taskDAO.getTaskById( tmpTask.id, next);
                },
            ], function(err) {
                assert(gotHere);
                assert(err instanceof Errors.InvalidTask);

                taskDAO.redisTaskHashExpireAt = redisTaskHashExpireAt;

                done();
            });
        });
    });

    describe('#removeTaskById', function(){
        it('can remove a Task in place provided a valid id', function(done){
            var tmpTask = taskFixtures[0];

            async.waterfall([
                function(next) {
                    TestHelpers.saveTasks(taskDAO, taskFixtures, next);
                },
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, next);
                },
                function(next) {
                    taskDAO.removeTaskById( tmpTask.id, next);
                },
                function(res, next) {
                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length - 1, next);
                },
            ], done);
        });

        it('does not fail if the Task does not exist', function(done){
            async.waterfall([
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, 0, next);
                },
                function(next) {
                    taskDAO.removeTaskById( NO_SUCH_TASK, next);
                },
                function(res, next) {
                    TestHelpers.assertTaskCount(taskDAO, 0, next);
                },
            ], done);
        });
    });

    describe('#removeTasksMulti', function(){
        it('can remove many Tasks in place', function(done){
            var ids = _und.pluck(taskFixtures, 'id');

            async.waterfall([
                function(next) {
                    TestHelpers.saveTasks(taskDAO, taskFixtures, next);
                },
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, taskFixtures.length, next);
                },
                function(next) {
                    taskDAO.removeTasksMulti( ids, next);
                },
                function(res, next) {
                    TestHelpers.assertTaskCount(taskDAO, 0, next);
                },
            ], done);
        });

        it('does not fail if a Task does not exist', function(done){
            async.waterfall([
                function(next) {
                    TestHelpers.assertTaskCount(taskDAO, 0, next);
                },
                function(next) {
                    taskDAO.removeTasksMulti( [ NO_SUCH_TASK ], next);
                },
            ], done);
        });
    });

    describe('#removeTaskMembersInRange', function(){
        it('can remove tasks provided a scheduled range', function(done){

            var tmpTask = Task.create(taskFixtures[1]);
            var tmpScheduledFor = tmpTask.scheduledFor;
            var ll = tmpScheduledFor - (1000 * 60), ul = tmpScheduledFor + (1000 * 60);

            taskDAO.saveTask( tmpTask, function(err){
                assert.ifError(err);
                taskDAO.removeTaskMembersInRange(ll, ul, function(err){
                    assert.ifError(err);
                    taskDAO.getTaskById( tmpTask.id, function(err, task){
                        assert.ifError(err);
                        assert(task);
                        done();
                    });
                });
            });
        });

    });

    describe('#removeAllTasks', function(){
        it('can remove many task in place', function(done){

            taskDAO.saveTasksMulti( taskFixtures, function(err, res){
                assert.ifError(err);
                assert(res);

                taskDAO.removeAllTasks(function(err){
                    assert.ifError(err);
                    done();
                });
            });
        });
    });

});

