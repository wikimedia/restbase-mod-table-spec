"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = module.parent.router;
var deepEqual = require('../utils/test_utils.js').deepEqual;

describe('Varint tables', function() {

    var varintTableSchema = {
        // keep extra redundant info for primary bucket table reconstruction
        domain: 'restbase.cassandra.test.local',
        table: 'varintTable',
        options: { durability: 'low' },
        attributes: {
            key: 'string',
            rev: 'varint',
            test: 'varint'
        },
        index: [
            { attribute: 'key', type: 'hash' },
            { attribute: 'rev', type: 'range', order: 'desc' },
            { attribute: 'test', type: 'range', order: 'desc' }
        ]
    };

    before(function () { return router.setup(); });
    it('creates varint table', function() {
        this.timeout(10000);
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable',
            method: 'put',
            body: varintTableSchema
        })
        .then(function(response) {
            deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable',
                method: 'get',
                body: {}
            });
        })
        .then(function(result) {
            deepEqual(result.status, 200);
            deepEqual(result.body, varintTableSchema);
        });
    });
    it('retrieves using varint predicates', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'put',
            body: {
                table: 'varintTable',
                consistency: 'localQuorum',
                attributes: {
                    key: 'testing',
                    rev: 1,
                    test: 1
                }
            }
        })
        .then(function(item) {
            deepEqual(item, {status: 201});
        })
        .then(function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'put',
                body: {
                    table: 'varintTable',
                    attributes: {
                        key: 'testing',
                        rev: 5,
                        test: 5
                    }
                }
            });
        })
        .then(function(item) {
            deepEqual(item, {status: 201});
        })
            // Simple query
        .then(function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
                method: 'get',
                body: {
                    table: 'varintTable',
                    limit: 3,
                    attributes: {
                        key: 'testing',
                        rev: 1
                    }
                }
            });
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
            deepEqual(result.body.items[0].key, 'testing');
            deepEqual(result.body.items[0].rev, 1);
        });
    });

    it('retrieves using eq predicate', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: { eq: 1 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
            deepEqual(result.body.items[0].key, 'testing');
            deepEqual(result.body.items[0].rev, 1);
        });
    });

    it('retrieves using lt predicate', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: { lt: 2 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
            deepEqual(result.body.items[0].key, 'testing');
            deepEqual(result.body.items[0].rev, 1);
        });
    });

    it('retrieves using gt predicate', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: { gt: 1 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
            deepEqual(result.body.items[0].key, 'testing');
            deepEqual(result.body.items[0].rev, 5);
        });
    });

    it('retrieves using le predicate', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: { le: 5 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 2);
        });
    });

    it('retrieves using ge predicate', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: { ge: 1 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 2);
        });
    });

    it('retrieves using multiple predicates', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: 1,
                    test: { ge: 1 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
            deepEqual(result.body.items[0].key, 'testing');
            deepEqual(result.body.items[0].rev, 1);
        });
    });

    it('retrieves using multiple non-eq predicates', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: 1,
                    test: {
                        ge: 1,
                        lt: 5
                    }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.body.items.length, 1);
            deepEqual(result.body.items[0].key, 'testing');
            deepEqual(result.body.items[0].rev, 1);
        });
    });

    it('fails on multiple non-eq predicates on different columns', function() {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/varintTable/',
            method: 'get',
            body: {
                table: 'varintTable',
                limit: 3,
                attributes: {
                    key: 'testing',
                    rev: { lt: 3 },
                    test: { ge: 1 }
                }
            }
        })
        .then(function(result) {
            deepEqual(result.status, 500);
        });
    });

    it('drops table', function() {
        this.timeout(15000);
        return router.request({
            uri: "/restbase.cassandra.test.local/sys/table/varintTable",
            method: "delete",
            body: {}
        })
        .then(function(res) {
            deepEqual(res.status, 204);
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/varintTable",
                method: "get",
                body: {}
            });
        })
        .then(function(res) {
            deepEqual(res.status, 500);
        });
    });
});