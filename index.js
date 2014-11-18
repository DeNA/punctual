"use strict";

var path = require('path');

// Core components
exports.Task = require( path.resolve(__dirname, './lib/Task') );
exports.TaskRunner = require( path.resolve(__dirname, './lib/TaskRunner') );
exports.TaskScheduler = require( path.resolve(__dirname, './lib/TaskScheduler') );
exports.TaskDAO = require( path.resolve(__dirname, './lib/TaskDAO') );

// Secondary components & helpers
exports.TaskUtils = require( path.resolve(__dirname, './lib/TaskUtils') );
exports.Errors = require( path.resolve(__dirname, './lib/Errors') );

exports.version = require( path.resolve(__dirname, './package.json') ).version;
