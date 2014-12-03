#!/usr/bin/env node

"use strict";

var punctual = require('../index.js');

var TaskScheduler = punctual.TaskScheduler;
var TaskRunner = punctual.TaskRunner;

console.log('Worker Alive: ' + process.pid);

var scriptTaskRunner = TaskRunner.create({ taskTypes: [ 'testType' ] });

TaskRunner.prototype.runTask = function(task) {
    console.log(JSON.parse(task.data));
};

var scheduler = TaskScheduler.create({
    logger: global.logger,
    pollIntervalLength: 250, // 1/4 sec poll
    redisKeys: {
        scheduledJobsZset: 'sweet:scheduler',
        scheduledJobsHash: function(jobId){ return 'sweet:scheduler:'+jobId.toString(); }
    },
    taskRunners: {
        scriptTaskRunner: scriptTaskRunner
    }
}).start();
