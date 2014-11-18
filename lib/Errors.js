"use strict";

var util = require('util'),
    _und = require('underscore');

/**
 * @constructor
 * @inherits Error
 */
function AbstractError(msg, constr) {
    Error.captureStackTrace(this, constr || this);
    this.name = this.name || "AbstractError";
    this.message = this.name + "::" + msg;
}

util.inherits(AbstractError, Error);
AbstractError.prototype.name = 'Abstract Error';


/**
 * @constructor
 * @inherits AbstractError
 */
function LogicError(code) {
    this.name = "LogicError";

    LogicError.super_.call(this, code);

    this.message = null;

    if(_und.isString(code) && code !== '') this.code = code;
    else if( _und.isObject(code) && _und.isString(code.code) && code.code !== '') this.code = code.code;
    else this.code = this.name;
}

util.inherits(LogicError, AbstractError);


/**
 * @constructor
 * @inherits AbstractError
 */
function RedisError(message) {
    this.name = "RedisError";
    RedisError.super_.call(this, message, this.constructor);
}

util.inherits(RedisError, AbstractError);


/**
 * @constructor
 * @inherits AbstractError
 */
function PollingError(message){
    this.name = "PollingError";
    PollingError.super_.call(this, message, this.constructor);
}

util.inherits(PollingError, AbstractError);


/**
 * @constructor
 * @inherits AbstractError
 */
function InvalidTaskRunner(message){
    this.name = "InvalidTaskRunner";
    InvalidTaskRunner.super_.call(this, message, this.constructor);
}

util.inherits(InvalidTaskRunner, AbstractError);


/**
 * @constructor
 * @inherits AbstractError
 */
function RunTaskError(message){
    this.name = "RunTaskError";
    RunTaskError.super_.call(this, message, this.constructor);
}

util.inherits(RunTaskError, AbstractError);


/**
 * @constructor
 * @inherits AbstractError
 */
function InvalidTask(message){
    this.name = "InvalidTask";
    InvalidTask.super_.call(this, message, this.constructor);
}

util.inherits(InvalidTask, AbstractError);

exports.UnknownError = "UnknownError";
exports.LogicError = LogicError;
exports.RedisError = RedisError;
exports.PollingError = PollingError;
exports.InvalidTaskRunner = InvalidTaskRunner;
exports.InvalidTask = InvalidTask;
exports.RunTaskError = RunTaskError;
