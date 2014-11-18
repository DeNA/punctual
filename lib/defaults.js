"use strict";

/**
 * Default configuration values for TaskScheduler + DAO
 */
var defaults = {
    // Default Redis keys to use for scheduler task ZSET and hashes
    redisKeys: {
        scheduledJobsZset: 'scheduler:tasks',
        scheduledJobsHash: function(jobId){ return 'scheduler:tasks:'+jobId.toString(); }
    },

    // Seconds after the task is run to keep the task hash in Redis
    redisTaskHashExpireAt: (60 * 60 * 24 * 3),         // 3 days in seconds
    redisPubSubChannel: 'scheduler',
    redisPubSubEnabled: true,

    /**
     * Extends console logging methods
     */
    logger: {
        debug: function(){ return console.log.apply(console, arguments); },
        info:  function(){ return console.info.apply(console, arguments); },
        warn:  function(){ return console.warn.apply(console, arguments); },
        error: function(){ return console.error.apply(console, arguments); }
    },

    pollIntervalLength: (1000 * 30),      // 30 seconds in ms

    redis: {
        port: 6379,
        host: '127.0.0.1',
        options: {}
    }
};

module.exports = defaults;
