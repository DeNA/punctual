#!/usr/bin/env node

"use strict";

var spawn = require('child_process').spawn;
var punctual = require('../index.js');

var WORKER_COUNT = 4;

var taskDAO = punctual.TaskDAO.create({
    redisKeys: {
        scheduledJobsZset: 'sweet:scheduler',
        scheduledJobsHash: function(jobId){ return 'sweet:scheduler:'+jobId.toString(); }
    }
});

var workers = [];

// Spawn several workers
for (var i = 0; i < WORKER_COUNT; i++) {
    (function() {
        var worker = spawn('./worker.js');

        worker.stdout.on('data', function (data) {
            console.log('STDOUT (' + worker.pid + '): ' + data);
        });

        worker.stderr.on('data', function (data) {
            console.log('STDERR' + worker.pid + '): ' + data);
        });

        workers.push(worker);

        console.log("New Worker: " + worker.pid);
    }());
}

var task_id = 1;

var addTaskTimer = setInterval(function() {
    console.log("Adding task " + task_id + "...");

    var newTask = punctual.Task.create({
        type: 'testType',
        scheduledFor: (new Date().getTime() + 1000), // 1s in future
        environment: 'development',
        tagger: 'anyonymous',
        active: true,
        data: JSON.stringify({
            test: true,
            myId: task_id
        }),
        createdAt: (new Date().getTime())
    });

    taskDAO.saveTask( newTask, function(err, task){
        console.log("New Task Created!");
    });

    if (++task_id >= 10) {
        clearTimeout(addTaskTimer);
    }
}, 1000);

setTimeout(function() {
    console.log("Killing Workers...");

    for (var i = 0; i < workers.length; i++) {
        workers[i].kill();
    }

    taskDAO.destroy(function() { /* TaskDAO.destroy() needs a callback */ });
}, 11 * 1000);