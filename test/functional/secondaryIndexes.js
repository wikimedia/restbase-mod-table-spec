"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = module.parent.router;
var deepEqual = require('../utils/test_utils.js').deepEqual;
var TimeUuid = require('cassandra-uuid').TimeUuid;

describe('Indices', function() {

    before(function () { return router.setup(); });

    var unversionedSecondaryIndexTableSchema = {
        table: 'unversionedSecondaryIndexTable',
        options: { durability: 'low' },
        attributes: {
            key: 'string',
            //tid: 'timeuuid',
            latestTid: 'timeuuid',
            uri: 'string',
            body: 'blob',
            // 'deleted', 'nomove' etc?
            restrictions: 'set<string>',
        },
        index: [
            { attribute: 'key', type: 'hash' },
            { attribute: 'uri', type: 'range', order: 'desc' },
        ],
        secondaryIndexes: {
            by_uri : [
                { attribute: 'uri', type: 'hash' },
                { attribute: 'key', type: 'range', order: 'desc' },
                { attribute: 'body', type: 'proj' }
            ]
        }
    };
    var simpleSecondaryIndexSchema = {
        table: 'simpleSecondaryIndexTable',
        options: { durability: 'low' },
        attributes: {
            key: 'string',
            tid: 'timeuuid',
            latestTid: 'timeuuid',
            uri: 'string',
            body: 'blob',
            // 'deleted', 'nomove' etc?
            restrictions: 'set<string>',
        },
        index: [
            { attribute: 'key', type: 'hash' },
            { attribute: 'tid', type: 'range', order: 'desc' },
        ],
        secondaryIndexes: {
            by_uri : [
                { attribute: 'uri', type: 'hash' },
                { attribute: 'body', type: 'proj' }
            ]
        }
    };
    var secondaryIndexSchemaWithRangeKeys = {
        table: 'secondaryIndexSchemaWithRangeKeys',
        options: { durability: 'low' },
        attributes: {
            key: 'string',
            range: 'int',
            tid: 'timeuuid',
            uri: 'string',
            body: 'string'
        },
        index: [
            { attribute: 'key', type: 'hash' },
            { attribute: 'range', type: 'range', order: 'desc' },
            { attribute: 'tid', type: 'range', order: 'desc' }
        ],
        secondaryIndexes: {
            by_uri : [
                { attribute: 'uri', type: 'hash' },
                { attribute: 'body', type: 'proj' }
            ]
        }
    };

    context('Secondary indices', function() {
        it('creates a secondary index table', function() {
            this.timeout(15000);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable',
                method: 'put',
                body: simpleSecondaryIndexSchema
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable',
                    method: 'get',
                    body: {}
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body, simpleSecondaryIndexSchema);
            });
        });
        it('successfully updates index', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "simpleSecondaryIndexTable",
                    attributes: {
                        key: "test",
                        tid: TimeUuid.now().toString(),
                        uri: "uri1",
                        body: 'body1'
                    },
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test",
                            tid: TimeUuid.now().toString(),
                            uri: "uri2",
                            body: 'body2'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test",
                            tid: TimeUuid.now().toString(),
                            uri: "uri3",
                            body: 'body3'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now().toString(),
                            uri: "uri1",
                            body: 'test_body1'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now().toString(),
                            uri: "uri2",
                            body: 'test_body2'
                        },
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now().toString(),
                            uri: "uri3",
                            body: 'test_body3'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'put',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        attributes: {
                            key: "test2",
                            tid: TimeUuid.now().toString(),
                            uri: "uri3",
                            // Also test projection updates
                            body: 'test_body3_modified'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status:201});
            });
        });
        it('retrieves rows with paging enabled', function() {
            return router.request({
                uri:'/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                method: 'get',
                body: {
                    table: "simpleSecondaryIndexTable",
                    limit: 2,
                    attributes: {
                        key: 'test2',
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 2);
                var next = response.body.next;
                return router.request({
                    uri:'/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/',
                    method: 'get',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        limit: 2,
                        next: next,
                        attributes: {
                            key: 'test2',
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 2);
                var next = response.body.next;
                return router.request({
                    uri:'/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTables/',
                    method: 'get',
                    body: {
                        table: "simpleSecondaryIndexTable",
                        limit: 1,
                        next: next,
                        attributes: {
                            key: 'test2',
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 0);
            });
        });
        it("throws 404 for values that no longer match", function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/",
                method: "get",
                body: {
                    table: "simpleSecondaryIndexTable",
                    index: "by_uri",
                    attributes: {
                        uri: "uri1"
                    }
                }
            })
            .then(function(response){
                deepEqual(response.status, 404);
                deepEqual(response.body.items.length, 0);
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/",
                    method: "get",
                    body: {
                        table: "simpleSecondaryIndexTable",
                        index: "by_uri",
                        attributes: {
                            uri: "uri2"
                        }
                    }
                });
            })
            .then(function(response){
                deepEqual(response.body.items.length, 0);
            });
        });
        it("retrieves the current value", function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable/",
                method: "get",
                body: {
                    table: "simpleSecondaryIndexTable",
                    index: "by_uri",
                    attributes: {
                        uri: "uri3"
                    },
                    proj: ['key', 'uri', 'body']
                }
            })
            .then(function(response){
                deepEqual(response.body.items, [{
                    key: "test2",
                    uri: "uri3",
                    body: new Buffer("test_body3_modified")
                },{
                    key: "test",
                    uri: "uri3",
                    body: new Buffer("body3")
                }]);
            });
        });
        it('does not override values with different range keys', function() {
            // Corner-case test for SQLite implementation, when a main table contains a range key which is
            // not tid and not a part of secondary index keys. Verifies that the secondary index entry is not
            // overwritten if two writes in a main table differs only in the described range key.
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/secondaryIndexSchemaWithRangeKeys',
                method: 'put',
                body: secondaryIndexSchemaWithRangeKeys
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/secondaryIndexSchemaWithRangeKeys/',
                    method: 'put',
                    body: {
                        table: "secondaryIndexSchemaWithRangeKeys",
                        attributes: {
                            key: "test1",
                            range: 1,
                            tid: TimeUuid.now().toString(),
                            uri: "uri1",
                            body: 'body1'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/secondaryIndexSchemaWithRangeKeys/',
                    method: 'put',
                    body: {
                        table: "secondaryIndexSchemaWithRangeKeys",
                        attributes: {
                            key: "test1",
                            range: 2,
                            tid: TimeUuid.now().toString(),
                            uri: "uri2",
                            body: 'body2'
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/secondaryIndexSchemaWithRangeKeys/",
                    method: "get",
                    body: {
                        table: "secondaryIndexSchemaWithRangeKeys",
                        index: "by_uri",
                        attributes: {
                            uri: "uri1"
                        },
                        proj: ['key', 'uri', 'body']
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'test1');
                deepEqual(response.body.items[0].uri, 'uri1');
                deepEqual(response.body.items[0].body.toString(), 'body1');

            })
        });
        this.timeout(15000);
        it('successfully drop secondary index tables', function() {
            return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable",
                    method: "delete",
                    body: {}
            })
            .then(function(res) {
                deepEqual(res.status, 204);
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/simpleSecondaryIndexTable",
                    method: "get",
                    body: {}
                })
            })
            .then(function(res) {
                deepEqual(res.status, 500);
            });
        });
    });

    context('Unversioned secondary indices', function() {
        it('creates a secondary index table with no tid in range', function() {
            this.timeout(8000);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable',
                method: 'put',
                body: unversionedSecondaryIndexTableSchema
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable',
                    method: 'get',
                    body: {}
                });
            })
            .then(function(response) {
                //console.log(response);
                deepEqual(response.status, 200);
                deepEqual(response.body, unversionedSecondaryIndexTableSchema);
            });
        });
        it('inserts into an unversioned index', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "unversionedSecondaryIndexTable",
                    attributes: {
                        key: "another test",
                        uri: "a uri",
                        body: "a body"
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable/',
                    method: 'get',
                    body: {
                        table: "unversionedSecondaryIndexTable",
                        index: "by_uri",
                        attributes: {
                            uri: "a uri"
                        },
                        proj: ['key', 'uri', 'body']
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'another test');
                deepEqual(response.body.items[0].uri, 'a uri');
                deepEqual(response.body.items[0].body.toString(), 'a body');
            });
        });
        it('updates an unversioned index', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable/',
                method: 'put',
                body: {
                    table: "unversionedSecondaryIndexTable",
                    attributes: {
                        key: "another test",
                        uri: "a uri",
                        body: "abcd"
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status:201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable/',
                    method: 'get',
                    body: {
                        table: "unversionedSecondaryIndexTable",
                        index: "by_uri",
                        attributes: {
                            uri: "a uri"
                        },
                        proj: ['key', 'uri', 'body']
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'another test');
                deepEqual(response.body.items[0].uri, 'a uri');
                deepEqual(response.body.items[0].body.toString(), 'abcd');
            });
        });
        this.timeout(15000);
        it('successfully drop unversioned secondary index tables', function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable",
                method: "delete",
                body: {}
            })
            .then(function(res) {
                deepEqual(res.status, 204);
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/unversionedSecondaryIndexTable",
                    method: "get",
                    body: {}
                })
            })
            .then(function(res) {
                deepEqual(res.status, 500);
            });
        });
    });
});
