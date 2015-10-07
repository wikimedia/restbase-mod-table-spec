"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = module.parent.router;
var utils = require('../utils/test_utils.js');
var deepEqual = utils.deepEqual;
var P = require('bluebird');

describe('Invalid request handling', function() {
    before(function () { return router.setup(); });

    after(function() {
        return P.all(['extraFieldSchema', 'orderTest'].map(function(schemaName) {
            return router.request({
                method: 'delete',
                uri: '/restbase.cassandra.test.local/sys/table/' + schemaName
            });
        }));
    });

    it('fails when writing to non-existent table', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/unknownTable/',
            method: 'put',
            body: {
                table: 'unknownTable',
                attributes: {
                    key: 'testing',
                    tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                }
            }
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('fails when reading from non-existent table', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/unknownTable/',
            method: 'get',
            body: {
                table: 'unknownTable',
                attributes: {
                    key: 'testing',
                    tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                }
            }
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('Fails to create static column that is a hash key', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/staticTest',
            method: 'put',
            body: {
                domain: 'restbase.cassandra.test.local',
                table: 'staticTest',
                attributes: {
                    key: 'string',
                    tid: 'timeuuid'
                },
                index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'key', type: 'static' },
                    { attribute: 'tid', type: 'range', order: 'desc' }
                ]
            }
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('Fails to order on non-range column', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/orderTest',
            method: 'put',
            body: {
                domain: 'restbase.cassandra.test.local',
                table: 'orderTest',
                attributes: {
                    key: 'string',
                    tid: 'timeuuid',
                    custom: 'string'
                },
                index: [
                    { attribute: 'key', type: 'hash' },
                    { attribute: 'tid', type: 'range', order: 'desc' }
                ]
            }
        })
        .then(function(response) {
            deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/orderTest/',
                method: 'get',
                body: {
                    table: 'orderTest',
                    attributes: {
                        key: 'string'
                    },
                    order: {
                        custom: 'asc'
                    }
                }
            });
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('Fails to make a query on non-existing index', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/orderTest/',
            method: 'get',
            body: {
                table: 'orderTest',
                attributes: {
                    key: 'string'
                },
                index: 'not_existing!'
            }
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('Validates order keys', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/orderTest/',
            method: 'get',
            body: {
                table: 'orderTest',
                attributes: {
                    key: 'string'
                },
                order: {
                    tid: "this_is_wronf"
                }
            }
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('Fails to create table without attributes', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/noAttrSchema',
            method: 'put',
            body: {
                domain: 'restbase.cassandra.test.local',
                table: 'noAttrSchema'
            }
        })
        .then(function(response) {
            deepEqual(response.status, 500);
        });
    });

    it('fails to insert unknown field', function() {
        return router.request({
            method: 'put',
            uri: '/restbase.cassandra.test.local/sys/table/extraFieldSchema',
            body: {
                domain: 'restbase.cassandra.test.local',
                table: 'extraFieldSchema',
                attributes: {
                    key: 'string',
                    value: 'string'
                },
                index: [
                    { attribute: 'key', type: 'hash' }
                ]
            }
        })
        .then(function(res) {
            deepEqual(res.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/extraFieldSchema/',
                method: 'put',
                body: {
                    table: 'extraFieldSchema',
                    attributes: {
                        key: 'key',
                        value: 'value',
                        extra_field: 'extra_value'
                    }
                }
            });
        })
        .then(function(res) {
            deepEqual(res.status, 500);
        });
    });
});
