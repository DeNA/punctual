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
