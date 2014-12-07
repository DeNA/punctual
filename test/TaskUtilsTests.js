"use strict";

exports.name = "TaskTests";

var assert = require('assert');
var TaskUtils = require('../lib/TaskUtils');

describe( exports.name, function(){

    describe('#getFunctionName()', function() {
        it('gets the name of a function', function(done){
            var myFunc = function myFunc () {};

            assert.equal('myFunc', TaskUtils.getFunctionName(myFunc));
            done();
        });
    });

});
