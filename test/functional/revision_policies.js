"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var P = require('bluebird');
var router = module.parent.router;
var assert = require('assert');
var utils = require('../utils/test_utils.js');

/** ensure a list of results contains exactly one matching entry */
function assertOne(items, tid) {
    var matched = items.filter(function(item) {
        return item.tid === tid;
    });

    assert(
        matched.length === 1,
        'expected 1 result with tid=' + tid + ', found ' + matched.length);
}

var testSchema = {
    table: 'revPolicyLatestTest',
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
    },
    revisionRetentionPolicy: {
        type: 'latest',
        count: 2,
        grace_ttl: 10
    }
};

var testSchemaNo2ary = {
    table: 'revPolicyLatestTest-no2ary',
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
    revisionRetentionPolicy: {
        type: 'latest',
        count: 2,
        grace_ttl: 10
    }
};

describe('MVCC revision policy', function() {
    before(function() {
        return router.setup()
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/' + testSchema.table,
                method: 'put',
                body: testSchema
            })
            .then(function(response) {
                assert.deepEqual(response.status, 201);
            })
            .then(function() {
                return router.request({
                    uri: '/domains_test/sys/table/' + testSchemaNo2ary.table,
                    method: 'put',
                    body: testSchemaNo2ary
                });
            })
            .then(function(response) {
                assert.deepEqual(response.status, 201);
            });
        });
    });

    after(function() {
        return router.request({
            uri: '/domains_test/sys/table/revPolicyLatestTest',
            method: 'delete',
            body: {}
        })
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/revPolicyLatestTest-no2ary',
                method: 'delete',
                body: {}
            });
        });
    });

    /* This is... tricky.
     * 
     * Since we do not (yet) want to expose TTLs in the table storage interface,
     * we're forced to infer that the correct TTL was set by waiting them out
     * and verifying that they disappear when expected.
     *
     * Additionally, we need to verify that the background updates are not
     * performing any unnecessary overwrites, thus we test in two phases with a
     * delay in between (the expectation being that their expiration will be
     * likewise staggered, unless the older TTL is clobbered in an overwrite).
     *
     * We write a total of four versions, with a 5 second gap between the write
     * time of the 3rd and 4th versions.  Since the revision policy specifies
     * that we retain the the last 2, and expire the remaining 2 after a
     * grace_ttl of 10 seconds, upon completion of the writes, the state should
     * look something like:
     * 
     * | record                                                          | TTL
     * +-----------------------------------------------------------------+-------
     * | { ..., tid: "2015-04-01 12:00:07-0500", comment: 'four times!'} | null
     * | { ..., tid: "2015-04-01 12:00:02-0500", comment: 'thrice'}      | null
     * | { ..., tid: "2015-04-01 12:00:01-0500", comment: 'twice'}       | 10
     * | { ..., tid: "2015-04-01 12:00:00-0500", comment: 'once'}        | 5
     * +-----------------------------------------------------------------+-------
     * 
     * At this point we wait an additional 6 seconds before performing a read,
     * at which point the state should look like the following:
     *
     * | record                                                          | TTL
     * +-----------------------------------------------------------------+-------
     * | { ..., tid: "2015-04-01 12:00:07-0500", comment: 'four times!'} | null
     * | { ..., tid: "2015-04-01 12:00:02-0500", comment: 'thrice'}      | null
     * | { ..., tid: "2015-04-01 12:00:01-0500", comment: 'twice'}       | 4
     * +-----------------------------------------------------------------+-------
     * 
     * The expectation is that in the intervening 6 seconds, the oldest entry
     * has expired, leaving us with only the most recent 3.
     *
     * After waiting an additional 5 seconds, we should have:
     *
     * | record                                                          | TTL
     * +-----------------------------------------------------------------+-------
     * | { ..., tid: "2015-04-01 12:00:07-0500", comment: 'four times!'} | null
     * | { ..., tid: "2015-04-01 12:00:02-0500", comment: 'thrice'}      | null
     * +-----------------------------------------------------------------+-------
     *
     */
    var revisionRetentionTest = function(test, tableName) {
        test.timeout(17000);
        return router.request({
            uri: '/domains_test/sys/table/'+tableName+'/',
            method: 'put',
            body: {
                table: tableName,
                consistency: 'localQuorum',
                attributes: {
                    title: 'Revisioned',
                    rev: 1000,
                    tid: utils.testTidFromDate(new Date("2015-04-01 12:00:00-0500")),
                    comment: 'once',
                    author: 'jsmith'
                }
            }
        })
        .then(function(response) {
            assert.deepEqual(response.status, 201);
        })
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/'+tableName+'/',
                method: 'put',
                body: {
                    table: tableName,
                    consistency: 'localQuorum',
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:01-0500")),
                        comment: 'twice',
                        author: 'jsmith'
                    }
                }
            });
        })
        .then(function(response) {
            assert.deepEqual(response, {status:201});

            return router.request({
                uri: '/domains_test/sys/table/'+tableName+'/',
                method: 'put',
                body: {
                    table: tableName,
                    consistency: 'localQuorum',
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:02-0500")),
                        comment: 'thrice',
                        author: 'jsmith'
                    }
                }
            });
        })
        .delay(5000)
        .then(function(response) {
            assert.deepEqual(response, {status:201});

            return router.request({
                uri: '/domains_test/sys/table/'+tableName+'/',
                method: 'put',
                body: {
                    table: tableName,
                    consistency: 'localQuorum',
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000,
                        tid: utils.testTidFromDate(new Date("2015-04-01 12:00:07-0500")),
                        comment: 'four times!',
                        author: 'jsmith'
                    }
                }
            });
        })
        .then(function(response) {
            assert.deepEqual(response, {status: 201});

            return router.request({
                uri: '/domains_test/sys/table/'+tableName+'/',
                method: 'get',
                body: {
                    table: tableName,
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000
                    }
                }
            });
        })
        // Delay long enough for the background updates to complete, then
        // for the grace_ttl of the oldest entry to expire.
        .delay(6000)
        .then(function(response) {
            // These assertions are for the GET performed immediately after the
            // 4 writes, TTL expirations may have occurred since, but these
            // results should reflect the entire set.
            assert.ok(response.body);
            assert.ok(response.body.items);
            assert.deepEqual(response.body.items.length, 4);

            return router.request({
                uri: '/domains_test/sys/table/'+tableName+'/',
                method: 'get',
                body: {
                    table: tableName,
                    attributes: {
                        title: 'Revisioned',
                        rev: 1000
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response.body);
            assert.ok(response.body.items);
            var items = response.body.items;
            assert.deepEqual(items.length, 3);
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:00:01-0500")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:00:02-0500")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:00:07-0500")));
            
            // Before issuing the final GET, delay an additional 5 seconds for
            // the next grace_ttl to expire.
            return P.delay(5000).then(function() {
                return router.request({
                    uri: '/domains_test/sys/table/'+tableName+'/',
                    method: 'get',
                    body: {
                        table: tableName,
                        attributes: {
                            title: 'Revisioned',
                            rev: 1000
                        }
                    }
                });
            });
        })
        .then(function(response) {
            assert.ok(response.body);
            assert.ok(response.body.items);
            var items = response.body.items;
            assert.deepEqual(items.length, 2);
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:00:02-0500")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:00:07-0500")));
        });
    };

    it('sets a TTL on all but the latest N entries (w/ 2ary index)', function() {
        return revisionRetentionTest(this, 'revPolicyLatestTest');
    });

    it('sets a TTL on all but the latest N entries (no 2ary indices)', function() {
        return revisionRetentionTest(this, 'revPolicyLatestTest-no2ary');
    });
});

