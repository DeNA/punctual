"use strict";

var _und = require('underscore'),
    util = require('util'),
    revalidator = require('revalidator'),
    Errors = require('./Errors'),
    TaskUtils = require('./TaskUtils');

/**
 *
 * @class Task
 * A base model for a task with schema and validation
 *
 * @constructor
 * @property {Object} taskObj
 * @config {string|number} [id]
 * @config {boolean} [active=true]
 * @config {string} [tagger='anonymous']
 * @config {string} type
 * @config {string} [subType] - further classification
 * @config {string|number|Date} [scheduledFor=now]
 * @config {number} [createdAt=now]
 */
function Task(taskObj){
    taskObj = taskObj || {};

    _und.extend(this, taskObj);

    return this;
}

module.exports = Task;


var schema = {
    properties: {
        id: {
            description: "Task identifier - maps task ZSET member to hash with task details.",
            type: ['string', 'number'],
            required: true,
            unique: true
        },
        active: {
            decription: "Is this task active?  Or disabled?  Do not run if this is false.",
            type: 'boolean',
            required: false
        },
        tagger: {
            description: 'Who scheduled this task? For logging purposes.',
            type: ['string', 'null'],
            required: false
        },
        type: {
            description: 'What type of task is this?  This should map to [taskTypes] in an appropriate TaskRunner.',         // script, gift, notification, config, event
            type: 'string',
            required: true
        },
        subType: {
            description: 'Any additional info about the type of this task?',
            type: ['string', 'null'],
            required: false
        },
        scheduledFor: {
            description: 'When should we trigger this action? Represented in milliseconds since epoch.',
            type: 'number',
            required: true
        },
        createdAt: {
            description: 'When was this task created? Represented in milliseconds since epoch.',
            type: 'number',
            required: false
        },
        data: {
            description: 'JSON serialed data for this specific cron task type',
            type: ['object', 'string', 'null'],
            required: false
        }
    }

};


/**
 * Create a new instance of Task.  
 * @param {Object} valuesObj
 * @param {Object} [opts] - optional params (Can pass in custom schema through opts.schema)
 * @param {function} [cb] - optional callback fxn to return function(error, taskInstance)
 */
Task.create = function(valuesObj, opts, cb) {
    if(_und.isFunction(opts)){ cb = opts; opts = {}; }
    opts = opts || {};

    var err, taskObj = {};

    taskObj.id = valuesObj.id || valuesObj._id || TaskUtils.cryptoHexIdStr();
    taskObj.active = (valuesObj.active === 'false' || valuesObj.active === false) ? false : true;

    // Convert scheduledFor attribute into an integer (can be passed as Date, String, or Float)
    if(_und.isNumber( parseInt(valuesObj.scheduledFor, 10) ) && !_und.isNaN( parseInt(valuesObj.scheduledFor, 10) ) ) taskObj.scheduledFor = parseInt(valuesObj.scheduledFor, 10);
    else if(_und.isString(valuesObj.scheduledFor) && _und.isDate( new Date(valuesObj.scheduledFor) ) ) taskObj.scheduledFor = new Date(valuesObj.scheduledFor).getTime();
    else if(_und.isDate( valuesObj.scheduledFor ) ) taskObj.scheduledFor = valuesObj.scheduledFor.getTime();
    else taskObj.scheduledFor = new Date().getTime();

    taskObj.createdAt = parseInt(valuesObj.createdAt) || parseInt(new Date().getTime());
    taskObj.tagger = valuesObj.tagger || null;
    taskObj.type = valuesObj.type;
    taskObj.subType = valuesObj.subType || null;

    //var data = _und.extend( valuesObj.data || {}, _und.omit(valuesObj, Object.keys(schema.properties) ) );
    //taskObj.data = JSON.stringify(data);

    // Extend all other passed-in attributes onto this task instance
    _und.extend( taskObj, _und.omit(valuesObj, Object.keys(schema.properties) ) );


    var v = revalidator.validate(taskObj, opts.schema || schema);
    if(!v.valid) {
        err = new Errors.InvalidTask('Task failed validation: ' + util.inspect(v.errors));
        if(opts.logger && _und.isFunction(opts.logger.error)) opts.logger.error(err);

        if(cb) return cb(err);
        else return false;
    }

    var newInst = new Task(taskObj);

    if(cb) return cb(null, newInst);
    else return newInst;
};
