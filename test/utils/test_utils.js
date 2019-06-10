'use strict';

var assert = require('assert');
var uuidv1 = require('uuid/v1');

var testUtils = {};

testUtils.deepEqual = function (result, expected) {
    try {
        assert.deepEqual(result, expected);
    } catch (e) {
        console.log('Expected:\n' + JSON.stringify(expected, null, 2));
        console.log('Result:\n' + JSON.stringify(result, null, 2));
        throw e;
    }
};

testUtils.roundDecimal = function (item) {
    return Math.round(item * 100) / 100;
};

testUtils.testTidFromDate = function testTidFromDate(date) {
    const options = {
        node: [0x01, 0x23, 0x45, 0x67, 0x89, 0xab],
        clockseq: 0x1234,
        msecs: date.getTime(),
        nsecs: 0
    };

    return uuidv1(options);
};

module.exports = testUtils;
