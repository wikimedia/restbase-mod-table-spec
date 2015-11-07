"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = module.parent.router;
var assert = require('assert');
var extend = require('extend');
var utils = require('../utils/test_utils.js');

function clone(obj) {
    return extend(true, {}, obj);
}

var testTable0 = {
    domain: 'restbase.cassandra.test.local',
    table: 'testTable0',
    options: { durability: 'low' },
    attributes: {
        title: 'string',
        rev: 'int',
        tid: 'timeuuid',
        comment: 'string',
        author: 'string'
    },
    index: [
        { attribute: 'title', type: 'hash' },
        { attribute: 'rev', type: 'range', order: 'desc' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ],
    secondaryIndexes: {
        by_rev : [
            { attribute: 'rev', type: 'hash' },
            { attribute: 'tid', type: 'range', order: 'desc' },
            { attribute: 'title', type: 'range', order: 'asc' },
            { attribute: 'comment', type: 'proj' }
        ]
    }
};

describe('Schema migration', function() {
    before(function() {
        return router.setup()
        .then(function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'PUT',
                body: testTable0
            })
            .then(function(response) {
                assert.ok(response, 'undefined response');
                assert.deepEqual(response.status, 201);
            });
        });
    });

    after(function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'delete'
        });
    });

    it('migrates revision retention policies', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 2;
        newSchema.revisionRetentionPolicy = {
            type: 'latest',
            count: 5,
            grace_ttl: 86400
        };

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);

            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET',
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(response.body, newSchema);
        });
    });

    it('requires monotonically increasing versions', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 1;
        newSchema.revisionRetentionPolicy = { type: 'all' };

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 400);
            assert.ok(
                /version must be higher|but no version increment/.test(response.body.title),
                'error message looks wrong');
        });
    });

    it('handles column additions', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 3;
        newSchema.attributes.email = 'string';

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET'
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(response.body, newSchema);
            // Verify that we can write to just added column
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'put',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'add_test',
                        rev: 1,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                        email: 'test'
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'get',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'add_test',
                        rev: 1
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(response.body.items[0].email, 'test');
        });
    });

    it('handles column removals', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 4;
        delete newSchema.attributes.author;

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);

            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET',
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(response.body, newSchema);
        });
    });

    it('refuses to remove indexed columns', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 5;
        delete newSchema.attributes.title;

        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'PUT',
            body: newSchema
        })
        .then(function(response) {
            assert.deepEqual(response.status, 500);
            assert.ok(
                    /is not in attributes/.test(response.body.stack),
                    'error message looks wrong');
        });
    });

    it('handles adding static columns', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 6;
        newSchema.attributes.added_static_column = 'string';
        newSchema.index.push({ attribute: 'added_static_column', type: 'static' });
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'put',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET'
            });
        })
        .then(function(response) {
            assert.deepEqual(response.body, newSchema);
            // Also test that column is indeed static
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'put',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'test',
                        rev: 1,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                        added_static_column: 'test1'
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'put',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'test',
                        rev: 2,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                        added_static_column: 'test2'
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'get',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'test',
                        rev: 1
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(response.body.items[0].added_static_column, 'test2');
        });
    });

    // This is a no-op test for cassandra, but it's a different codepath in SQLite
    it('adds one more static column', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 7;
        newSchema.attributes.added_static_column = 'string';
        newSchema.index.push({ attribute: 'added_static_column', type: 'static' });
        newSchema.attributes.added_static_column2 = 'string';
        newSchema.index.push({ attribute: 'added_static_column2', type: 'static' });
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'put',
            body: newSchema
        })
        .then(function(response) {
            assert.ok(response);
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0',
                method: 'GET'
            });
        })
        .then(function(response) {
            assert.deepEqual(response.body, newSchema);
            // Also test that column is indeed static
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'put',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'test2',
                        rev: 1,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                        added_static_column2: 'test1'
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'put',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'test2',
                        rev: 2,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                        added_static_column2: 'test2'
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'get',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'test2',
                        rev: 1
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response, 'undefined response');
            assert.deepEqual(response.status, 200);
            assert.deepEqual(response.body.items[0].added_static_column2, 'test2');
        });
    });

    it('does not change static index on existing column', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 8;
        newSchema.index.push({attribute: 'not_static', type: 'static'});
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'put',
            body: newSchema
        })
        .then(function(response) {
            assert.deepEqual(response.status, 500);
        });
    });

    // We can't add a secondary index back, so this test must be last in the test set.
    it('removes a secondary index', function() {
        var newSchema = clone(testTable0);
        newSchema.version = 9;
        delete newSchema.secondaryIndexes.by_rev;
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/testTable0',
            method: 'put',
            body: newSchema
        })
        .then(function(response) {
            assert.deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'get',
                body: {
                    table: 'testTable0',
                    index: 'by_rev',
                    attributes: {
                        rev: 1
                    }
                }
            });
        })
        .then(function(response) {
            assert.deepEqual(response.status, 500);
            // And check that the table is still usable and index updates didn't break
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/testTable0/',
                method: 'put',
                body: {
                    table: 'testTable0',
                    attributes: {
                        title: 'add_test',
                        rev: 111,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500"))
                    }
                }
            });
        })
        .then(function(response) {
            assert.deepEqual(response.status, 201);
        });
    });
});
