"use strict";

var assert = require('assert'),
    async = require('async'),
    redis = require('redis'),
    taskFixtures = require('../fixtures/tasks');


exports.newRedisClient = function(port, host){
    return redis.createClient(port || 6379, host || '127.0.0.1');
};

exports.randomTaskFixture = function(){
    return taskFixtures[ Math.floor( Math.random() * taskFixtures.length ) ];
};

exports.assertTaskCount = function(taskDAO, count, cb) {
    // *sigh* this is NOT a count of Task hashes,
    //   it is a count of the number of scheduled Tasks
    taskDAO.getAllTasks({ keepTaskIds: true }, function(err, tasks) {
        assert.ifError(err);
        assert.equal(tasks.length, (count || 0));
        cb();
    });
};

exports.saveTasks = function(taskDAO, tasks, cb) {
    async.each(tasks, taskDAO.saveTask.bind(taskDAO), cb);
};

exports.clearTasks = function(taskDAO, cb) {
    taskDAO.getAllTasks({ keepTaskIds: false }, function(err) {
        assert.ifError(err);
        cb();
    });
};
