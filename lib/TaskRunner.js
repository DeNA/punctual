"use strict";

var _und = require('underscore'),
    util = require('util'),
    TaskUtils = require('./TaskUtils'),
    EventEmitter = require('events').EventEmitter,
    Errors = require('./Errors'),
    defaults = require('./defaults');


/**
 * A handler for running tasks of specific types
 *
 * Implementation should override these base functions and contain
 * any libraries and business-logic needed for executing those task types
 *
 * @constructor TaskRunner
 * @param { Object } opts
 * @config { function } [logger]
 * @config { String[] } [taskTypes]
 */
function TaskRunner(opts){
    opts = opts || {};

    if(_und.isBoolean(opts.logger) && !opts.logger) opts.logger = TaskUtils.noopLogger;
    this.logger = opts.logger || defaults.logger;

    // An array of string constants the TaskScheduler uses to map Tasks to this TaskRunner
    this.taskTypes = this.taskTypes || opts.taskTypes || [];

    // Our in-memory, temp active tasks list
    // Use for managing FIFO operations if this array is large.
    this._activeTasks = [];

    EventEmitter.call(this);
}

util.inherits(TaskRunner, EventEmitter);
module.exports = TaskRunner;


/**
 * Return a new TaskRunner instance
 * @param {Object} opts
 * @returns {TaskRunner}
 */
TaskRunner.create = function(opts){
    if( this instanceof TaskRunner ) return this;
    if( this.prototype instanceof TaskRunner ) return new this(opts);
    return new TaskRunner(opts);
};


/**
 * Convenience function to throw an error and callback if there is a cb function
 * @param {Error} err
 * @param {Function} [cb]
 */
TaskRunner.prototype.throwErr = function(err, cb){
    if(this.logger && _und.isFunction(this.logger.error)) this.logger.error( err );

    this.emit('error', err);

    if(cb) return cb(err);
    else return false;
};

/**
 * Registers a specific Task class to be handled by this TaskRunner instance
 * @param {Task} task
 * @return {Bool|String[]} - if unsuccesful return false, else return updated taskTypes
 */

TaskRunner.prototype.registerTask = function(task){
    // Task must have a type, this is used for the mapping
    if( ! (_und.isString(task.type) && task.type) ) {
        throw new Errors.InvalidTask("Task does not specify a type");
    }

    // Task type has already been registered, return original types array
    if( this.taskTypes.indexOf(task.type) !== -1) return this.taskTypes;

    this.taskTypes.push(task.type);

    return this.taskTypes;
};

/**
 * Must override this method inside TaskRunner class implementation
 */
TaskRunner.prototype.run = TaskRunner.prototype.runTask = function(task, opts, cb){
    //this._activeTasks.push(task);

    this.throwErr(new Errors.RunTaskError("Must override TaskRunner.runTask method in TaskRunner implementation"), cb);
};

TaskRunner.prototype.teardown = function(cb){
    if(cb) cb(null, { activeTasks: this._activeTasks });
};

TaskRunner.prototype.init = function(cb){
    this._activeTasks = this._activeTasks || [];
    if(cb) cb(null, this._activeTasks);
};

