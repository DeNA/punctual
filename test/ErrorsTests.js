"use strict";

exports.name = "TaskRunnerTests";

var assert = require('assert'),
    TaskRunner = require('../lib/TaskRunner'),
    TestHelpers = require('./helpers/TestHelpers'),
    Errors = require('../lib/Errors');


var redisClient;

describe( exports.name, function(){
    before(function(cb){
        redisClient = TestHelpers.newRedisClient();
        cb();
    });

    describe("#throwErr", function (){
        var taskRunner;
        beforeEach(function () {
            taskRunner = TaskRunner.create();
        });

        it('throws a LogicError', function () {
            assert.throws(function () {
                taskRunner.throwErr(new Errors.LogicError('An error occurred'));
            }, Errors.LogicError);
        });

        it('throws a LogicError with an object code', function () {
            assert.throws(function () {
                taskRunner.throwErr(new Errors.LogicError({code: 'An error occurred'}));
            }, Errors.LogicError);
        });

        it('throws a LogicError with a nonexistent code', function () {
            assert.throws(function () {
                taskRunner.throwErr(new Errors.LogicError());
            }, Errors.LogicError);
        });

        it('throws a PollingError', function () {
            assert.throws(function () {
                taskRunner.throwErr(new Errors.PollingError('An error occurred'));
            }, Errors.PollingError);
        });

        it('throws a RunTaskError', function () {
            assert.throws(function () {
                taskRunner.throwErr(new Errors.RunTaskError('An error occurred'));
            }, Errors.RunTaskError);
        });
    });
});

