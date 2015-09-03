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
        grace_ttl: 5
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
        grace_ttl: 5
    }
};

var testIntervalSchema2 = {
    table: 'revPolicyIntervalTest2',
    attributes: {
        title: 'string',
        rev: 'int',
        tid: 'timeuuid',
        comment: 'string'
    },
    index: [
        { attribute: 'title', type: 'hash' },
        { attribute: 'rev', type: 'range', order: 'desc' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ],
    revisionRetentionPolicy: {
        type: 'interval',
        interval: 86400,
        count: 2,
        grace_ttl: 2
    }
};

var testIntervalSchema = {
    table: 'revPolicyIntervalTest',
    attributes: {
        title: 'string',
        rev: 'int',
        tid: 'timeuuid',
        comment: 'string'
    },
    index: [
        { attribute: 'title', type: 'hash' },
        { attribute: 'rev', type: 'range', order: 'desc' },
        { attribute: 'tid', type: 'range', order: 'desc' }
    ],
    revisionRetentionPolicy: {
        type: 'interval',
        interval: 86400,
        count: 1,
        grace_ttl: 2
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
                return router.request({
                    uri: '/domains_test/sys/table/' + testSchemaNo2ary.table,
                    method: 'put',
                    body: testSchemaNo2ary
                });
            })
            .then(function(response) {
                assert.deepEqual(response.status, 201);
                return router.request({
                    uri: '/domains_test/sys/table/' + testIntervalSchema.table,
                    method: 'put',
                    body: testIntervalSchema
                });
            })
            .then(function(response) {
                assert.deepEqual(response.status, 201);
                return router.request({
                    uri: '/domains_test/sys/table/' + testIntervalSchema2.table,
                    method: 'put',
                    body: testIntervalSchema2
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
        })
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/' + testIntervalSchema.table,
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
        .delay(2500)
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
        .delay(3000)
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

            // Before issuing the final GET, delay an additional 2.5 seconds for
            // the next grace_ttl to expire.
            return P.delay(2500).then(function() {
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

    function createRenders(schema, title, revision, timestamps) {
        var index = 1;
        return P.each(timestamps, function(timestamp) {
            return router.request({
                uri: '/domains_test/sys/table/'+ schema.table +'/',
                method: 'put',
                body: {
                    table: schema.table,
                    attributes: {
                        title: title,
                        rev: revision,
                        tid: utils.testTidFromDate(timestamp),
                        comment: '#' + (index++)
                    }
                }
            })
            .then(function(response) {
                assert.deepEqual(response.status, 201);
            });
        });
    }

    it('sets a TTL on all but the latest N entries (w/ 2ary index)', function() {
        return revisionRetentionTest(this, 'revPolicyLatestTest');
    });

    it('sets a TTL on all but the latest N entries (no 2ary indices)', function() {
        return revisionRetentionTest(this, 'revPolicyLatestTest-no2ary');
    });

    // Checks interval rev retention policy: need to ensure we have max 2 renders every 24 hours
    // We add 3 renders on first day, 1 render on third day and 3 renders on the 4th day
    // Renders number 1 (first one is never deleted), 3, 4, 5, 7 must survive, others should get removed
    it('sets a TTL for interval rev policy', function() {
        this.timeout(5000);
        return createRenders(testIntervalSchema2, "Revisioned", 1000, [
            // Day 1: 3 renders come
            new Date("2015-04-01 12:00:00-0000"),
            new Date("2015-04-01 12:10:00-0000"),
            new Date("2015-04-01 12:50:00-0000"),
            // Next day - nothing
            // Next day - one revision comes
            new Date("2015-04-03 12:00:00-0000"),
            // Next day tree more
            new Date("2015-04-04 12:00:00-0000"),
            new Date("2015-04-04 12:30:00-0000"),
            new Date("2015-04-04 13:00:00-0000")
        ])
        .delay(2000)
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/'+ testIntervalSchema2.table +'/',
                method: 'get',
                body: {
                    table: testIntervalSchema2.table,
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
            assert.deepEqual(items.length, 5);
            // According to the algo the first ever render is never deleted
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:10:00-0000")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 12:50:00-0000")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-03 12:00:00-0000")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-04 12:30:00-0000")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-04 13:00:00-0000")));
        });
    });

    // Checks if the problem with sliding beginning happens for interval policy
    it('sets a TTL for interval rev policy', function() {
        this.timeout(5000);
        // Day 1: 3 renders come
        return createRenders(testIntervalSchema, "Sliding", 1001, [
            new Date("2015-04-01 05:00:00-0000"),
            new Date("2015-04-01 11:00:00-0000"),
            new Date("2015-04-01 17:00:00-0000"),
            new Date("2015-04-01 23:00:00-0000"),
            new Date("2015-04-02 05:00:00-0000"),
            new Date("2015-04-02 11:00:00-0000"),
            new Date("2015-04-02 17:00:00-0000"),
            new Date("2015-04-02 23:00:00-0000"),
            new Date("2015-04-03 05:00:00-0000")
        ])
        .delay(3000)
        .then(function() {
            return router.request({
                uri: '/domains_test/sys/table/'+ testIntervalSchema.table +'/',
                method: 'get',
                body: {
                    table: testIntervalSchema.table,
                    attributes: {
                        title: 'Sliding',
                        rev: 1001
                    }
                }
            });
        })
        .then(function(response) {
            assert.ok(response.body);
            assert.ok(response.body.items);
            var items = response.body.items;
            assert.deepEqual(items.length, 3);
            assertOne(items, utils.testTidFromDate(new Date("2015-04-01 23:00:00-0000")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-02 23:00:00-0000")));
            assertOne(items, utils.testTidFromDate(new Date("2015-04-03 05:00:00-0000")));
        });
    });
});