"use strict";

var crypto = require("crypto");

/**
 * Return a new ID string
 * @param {number} [numBytes=12]
 * @returns {string} - hexadecimal string
 */
exports.cryptoHexIdStr = function(numBytes){
    return crypto.randomBytes( numBytes || 12 ).toString('hex');
};

/**
 * Return a function's name
 * @param {function} fxn
 * @returns {string} funcStr
 */
exports.getFunctionName = function(fxn){
    var funcStr = fxn.toString();
    funcStr = funcStr.substr('function '.length);
    funcStr = funcStr.substr(0, funcStr.indexOf('('));
    return funcStr;
};

/**
 * Properly close a Redis client
 * @param {Object} client
 * @param {Function} cb
 */
exports.closeRedisClient = function(client, cb) {
    if (! (client && client.connected)) {
        return cb();
    }
    client.once('end', cb);
    client.quit();
};

exports.noopLogger = {
    debug: function(){},
    info: function(){},
    warn: function(){},
    error: function(){}
};
