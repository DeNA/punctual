"use strict";

var Task = require('../../lib/Task');

function randomTime() {
    // up to 100 minutes into the future
    return new Date().getTime() + (1000 * 60 * (Math.floor(Math.random() * 100 )));
}


module.exports = [
    Task.create({
        type: 'testType',
        scheduledFor: randomTime(),
        environment: 'development',
        tagger: 'anyonymous',
        active: true,
        data: {
            test: true
        },
        createdAt: (new Date().getTime())
    }),
    Task.create({
        type: 'testType',
        scheduledFor: randomTime(),
        environment: 'development',
        tagger: 'anyonymous',
        active: true,
        data: {
            test: true
        },
        createdAt: (new Date().getTime())
    })
];
