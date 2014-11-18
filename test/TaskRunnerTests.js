"use strict";

exports.name = "TaskRunnerTests";

var assert = require('assert'),
    util = require('util'),
    TestHelpers = require('./helpers/TestHelpers'),
    TaskRunner = require('../lib/TaskRunner'),
    Task = require('../lib/Task'),
    TaskUtils = require('../lib/TaskUtils'),
    taskFixtures = require('./fixtures/tasks');

var redisClient;

describe( exports.name, function(){
    before(function(cb){
        redisClient = TestHelpers.newRedisClient();
        cb();
    });

    describe('#create', function(){
        var OPTS = { logger: false };
        it('creates a new TaskRunner instance', function(){
            var taskRunner = TaskRunner.create(OPTS);
            assert( taskRunner instanceof TaskRunner );
            assert.strictEqual( taskRunner.logger, OPTS.logger );
        });

        it('creates a new sub-Class of TaskRunner', function(){
            // a sub-Class
            var MyRunner = function() {
                TaskRunner.apply(this, arguments);
            };
            util.inherits(MyRunner, TaskRunner);

            // obtuse way
            assert(TaskRunner.create.call(MyRunner) instanceof MyRunner);

            // Classy way
            MyRunner.create = TaskRunner.create;

            var taskRunner = MyRunner.create(OPTS);
            assert( taskRunner instanceof MyRunner );
            assert.strictEqual( taskRunner.logger, TaskUtils.noopLogger );
        });

        it('passes through the instance of a TaskRunner', function(){
            // a sub-Class
            var MyRunner = function() {
                TaskRunner.apply(this, arguments);
            };
            util.inherits(MyRunner, TaskRunner);

            // an instance of it
            MyRunner.create = TaskRunner.create;
            var taskRunner = MyRunner.create(OPTS);
            assert.strictEqual( taskRunner.logger, OPTS.logger );

            // obtuse way
            assert.strictEqual( TaskRunner.create.call(taskRunner), taskRunner );

            // Classy way
            MyRunner.prototype.create = TaskRunner.create;

            assert.strictEqual( taskRunner.create(), taskRunner );

            // options are ignored
            assert.strictEqual( taskRunner.create({ logger: 'ignored' }).logger, OPTS.logger );
        });

        it('respects #taskTypes already present on a TaskRunner', function(){
            // a sub-Class
            var MyRunner = function() {
                TaskRunner.apply(this, arguments);
            };
            util.inherits(MyRunner, TaskRunner);

            assert.deepEqual( new MyRunner({ taskTypes: [ 'OPTS' ] }).taskTypes, [ 'OPTS' ] );

            MyRunner.prototype.taskTypes = [ 'PROTO' ];
            assert.deepEqual( new MyRunner({ taskTypes: [ 'OPTS' ] }).taskTypes, [ 'PROTO' ] );
        });
    });

    describe("#registerTask", function(){
        var taskRunner;
        beforeEach(function() {
            taskRunner = TaskRunner.create();
        });

        it('registers as a handler for a specific task type', function(){
            taskRunner.registerTask(taskFixtures[0]);
            assert.ok(taskRunner.taskTypes.length = 1);
            assert.ok(taskRunner.taskTypes[0] === 'testType');
        });

        it('throws unless the task type is a non-blank String', function(){
            var task = new Task(taskFixtures[0]);

            [ null, 99, '' ].forEach(function(badType) {
                task.type = badType;
                assert.throws(function() {
                    taskRunner.registerTask(task);
                }, 'failed on: ' + badType);
            });

            // fixture remains unchanged
            assert.equal(taskFixtures[0].type, 'testType');
        });
    });
});

