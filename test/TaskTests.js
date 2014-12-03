"use strict";

exports.name = "TaskTests";

var assert = require('assert'),
    _und = require('underscore'),
    Task = require('../lib/Task');

describe( exports.name, function(){

    describe('#create()', function(){
        it('creates a new task instance provided required options', function(done){
            var task = Task.create({ type: 'testType', scheduledFor: (new Date().getTime()), message: 'testMsg', recipients: 'all' });

            assert.ok( task instanceof Task );
            assert.ok( _und.isNumber( task.scheduledFor ) );
            assert.ok( task.message === 'testMsg' );
            done();
        });
        it('creates a new task instance provided a date string in scheduledFor', function(done){
            var task = Task.create({ type: 'testType', scheduledFor: 'December 17, 1995 03:24:00', message: 'testMsg', recipients: 'all' });

            assert.ok( task instanceof Task );
            assert.ok( _und.isNumber( task.scheduledFor ) );
            assert.ok( task.message === 'testMsg' );
            done();
        });
        it('creates a new task with current time given a Date value in scheduledFor', function(done){
            var task = Task.create({ type: 'testType', scheduledFor: new Date(), message: 'testMsg', recipients: 'all' });

            assert.ok( task instanceof Task );
            assert.ok( _und.isNumber( task.scheduledFor ) );
            assert.ok( task.message === 'testMsg' );
            done();
        });
        it('creates a new task instance when passed two args', function(done){
            var taskAttr = {
                type: 'testType',
                scheduledFor: (new Date().getTime()),
                message: 'testMsg',
                recipients: 'all'
            };
            Task.create(taskAttr, function (err, task) {
                assert.ok( task instanceof Task );
                assert.ok( _und.isNumber( task.scheduledFor ) );
                assert.ok( task.message === 'testMsg' );
                done();
            });

        });
        it('creates a new task instance when passed three args', function(done){
            var taskAttr = {
                type: 'testType',
                scheduledFor: (new Date().getTime()),
                message: 'testMsg',
                recipients: 'all'
            };
            var opts = {
                someOption: true
            };
            Task.create(taskAttr, opts, function (err, task) {
                assert.ok( task instanceof Task );
                assert.ok( _und.isNumber( task.scheduledFor ) );
                assert.ok( task.message === 'testMsg' );
                done();
            });

        });
        it('fails to create a new task when it fails schema validation', function(done){
            var taskAttr = {
                scheduledFor: (new Date().getTime()),
                message: 'testMsg',
                recipients: 'all'
            };
            var opts = {
                someOption: true
            };
            Task.create(taskAttr, opts, function (err, task) {
                assert(err, 'Task.create throws an error');
                assert.equal('InvalidTask', err.name);
                assert.ok(!task, 'No task is created');
                done();
            });

        });
        it('fails to create a new task when it fails validation with a custom schema', function(done){
            var taskAttr = {
                scheduledFor: (new Date().getTime()),
                message: 'testMsg',
                recipients: 'all'
            };
            var opts = {
                schema: {
                    properties: {
                        newProperty: {
                            description: "A new property in a custom schema",
                            type: 'string',
                            required: true
                        }
                    }
                }
            };
            Task.create(taskAttr, opts, function (err, task) {
                assert(err, 'Task.create throws an error');
                assert.equal('InvalidTask', err.name);
                assert.ok(!task, 'No task is created');
                done();
            });

        });
        it('fails to create a new task when missing a type attribute', function(done){
            var task = Task.create({ scheduledFor: (new Date().getTime()), message: 'testMsg', recipients: 'all' });

            assert.ok( !(task instanceof Task) );
            assert.ok( !task );
            assert.ok( !_und.isNumber( task.scheduledFor ) );
            assert.ok( task.message !== 'testMsg' );
            done();
        });
    });

});
