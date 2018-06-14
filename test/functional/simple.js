"use strict";

// mocha defines to avoid JSHint breakage
/* global describe, context, it, before, beforeEach, after, afterEach */

var router = module.parent.router;
var deepEqual = require('../utils/test_utils.js').deepEqual;
var utils = require('../utils/test_utils.js');
var P = require('bluebird');

describe('Simple tables', function() {

    before(function() {
        return router.setup();
    });

    var simpleTableSchema = {
        table: 'simple-table',
        options: {
            durability: 'low'
        },
        attributes: {
            key: 'string',
            tid: 'timeuuid',
            latestTid: 'timeuuid',
            body: 'blob',
            'content-type': 'string',
            'content-length': 'varint',
            'content-sha256': 'string',
            // redirect
            'content-location': 'string',
            // 'deleted', 'nomove' etc?
            restrictions: 'set<string>',
        },
        index: [
            {attribute: 'key', type: 'hash'},
            {attribute: 'latestTid', type: 'static'},
            {attribute: 'tid', type: 'range', order: 'desc'}
        ]
    };

    after(function() {
        return router.request({
            uri: '/restbase1.cassandra.test.local/sys/table/simple-table',
            method: 'delete'
        });
    });

    context('Create', function() {
        before(function() {
            // Create a same table on different domain
            return router.request({
                uri: '/restbase1.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: simpleTableSchema
            })
            .then(function(response) {
                deepEqual(response.status, 201);
            });
        });

        it('creates a simple test table', function() {
            this.timeout(15000);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: simpleTableSchema
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                    method: 'get',
                    body: {}
                })
                .then(function(response) {
                    deepEqual(response.status, 200);
                    deepEqual(response.body, simpleTableSchema);
                });
            });
        });
        it('throws an error on unsupported schema update request', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table',
                method: 'put',
                body: {
                    table: 'simple-table',
                    options: {
                        durability: 'low'
                    },
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        body: 'blob',
                        'content-type': 'string',
                        'content-length': 'varint',
                        'content-sha256': 'string',
                        // redirect
                        'content-location': 'string',
                        // 'deleted', 'nomove' etc?
                        //
                        // NO RESTRICTIONS HERE
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'latestTid', type: 'static'},
                        {attribute: 'tid', type: 'range', order: 'desc'}
                    ]
                }
            }).then(function(response) {
                deepEqual(response.status, 400);
            });
        });
    });

    context('Put', function() {
        this.timeout(15000);

        it('inserts a row', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'testing',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
            });
        });
        it('updates a row', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: "testing",
                        tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                        body: new Buffer("<p>Service Oriented Architecture</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
            });
        });
        it('inserts using if-not-exists with non index attributes', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    if: "not exists",
                    attributes: {
                        key: "testing if not exists",
                        tid: utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
                        body: new Buffer("<p>if not exists with non key attr</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "testing if not exists",
                            tid: utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
                        }
                    }
                })
                .then(function(response) {
                    deepEqual(response.status, 200);
                    deepEqual(response.body.items.length, 1);
                    deepEqual(response.body.items[0].key, "testing if not exists");
                    deepEqual(response.body.items[0].tid,
                        utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')));
                    deepEqual(response.body.items[0].body,
                        new Buffer("<p>if not exists with non key attr</p>"));
                });
            });
        });
        it('does not replace using if-not-exist if exists', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    if: "not exists",
                    attributes: {
                        key: "testing if not exists",
                        tid: utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
                        body: new Buffer("<p>new body we wanted to replace</p>")
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "testing if not exists",
                            tid: utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700'))
                        }
                    }
                })
                .then(function(response) {
                    deepEqual(response.status, 200);
                    deepEqual(response.body.items.length, 1);
                    deepEqual(response.body.items[0].key, "testing if not exists");
                    deepEqual(response.body.items[0].tid,
                        utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')));
                    deepEqual(response.body.items[0].body,
                        new Buffer("<p>if not exists with non key attr</p>"));
                });
            });
        });
        it('inserts with if-condition and non index attributes', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: "another test",
                        tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')),
                        body: new Buffer("<p>Service Oriented Architecture</p>")
                    }
                }
            }).then(function() {
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "another test",
                            tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')),
                            body: new Buffer("<p>test<p>")
                        },
                        if: {body: {"eq": new Buffer("<p>Service Oriented Architecture</p>")}}
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "another test",
                            tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700'))
                        }
                    }
                })
                .then(function(response) {
                    deepEqual(response.status, 200);
                    deepEqual(response.body.items.length, 1);
                    deepEqual(response.body.items[0].key, 'another test');
                    deepEqual(response.body.items[0].tid,
                        utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')));
                    deepEqual(response.body.items[0].body, new Buffer("<p>test<p>"));
                });
            });
        });
        it ('does not inserts with if-condition in case condition is false', function() {
            // Now we have different body, so request shouldn't modify the resourse
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: "another test",
                        tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')),
                        body: new Buffer("<p>new test data<p>")
                    },
                    if: {body: {"eq": new Buffer("<p>Service Oriented Architecture</p>")}}
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "another test",
                            tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700'))
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'another test');
                deepEqual(response.body.items[0].tid,
                utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')));
                // The body was not modified
                deepEqual(response.body.items[0].body, new Buffer("<p>test<p>"));
            });
        });
        it('inserts static columns', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'test',
                        tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                        latestTid: utils.testTidFromDate(new Date('2014-01-01 00:00:00')),
                    }
                }
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'test2',
                            tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                            body: new Buffer("<p>test<p>"),
                            latestTid: utils.testTidFromDate(new Date('2014-01-01 00:00:00')),
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'test',
                            tid: utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
                            latestTid: utils.testTidFromDate(new Date('2014-01-02 00:00:00'))
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response, {status: 201});
            });
        });
        it('allows setting TTL for individual rows', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'ttl_test',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        body: new Buffer("<p>test<p>"),
                        _ttl: 3
                    }
                }
            })
            .then(function(res) {
                deepEqual(res.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'ttl_test'
                        }
                    }
                });
            })
            .then(function(res) {
                deepEqual(res.status, 200);
                deepEqual(res.body.items.length, 1);
            })
            .delay(5000)
            .then(function() {
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'ttl_test'
                        }
                    }
                });
            })
            .then(function(res) {
                deepEqual(res.status, 404);
            });
        });
        it('allows overriding columns', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'override_test',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        body: new Buffer("<p>test<p>")
                    }
                }
            })
            .then(function(res) {
                deepEqual(res.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'override_test',
                            tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                            body: new Buffer("<p>new_test<p>")
                        }
                    }
                });
            })
            .then(function(res) {
                deepEqual(res.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'override_test'
                        }
                    }
                });
            })
            .then(function(res) {
                deepEqual(res.status, 200);
                deepEqual(res.body.items.length, 1);
                deepEqual(res.body.items[0].body, new Buffer("<p>new_test<p>"));
            });
        });
    });

    context('Get', function() {
        it('retrieves a row', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: 'testing',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700'))
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items, [{
                    key: 'testing',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    latestTid: null,
                    body: null,
                    'content-length': null,
                    'content-location': null,
                    'content-sha256': null,
                    'content-type': null,
                    restrictions: null
                }]);
            });
        });
        it('retrieves using between condition', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    //from: 'foo', // key to start the query from (paging)
                    limit: 3,
                    attributes: {
                        key: "testing",
                        tid: {
                            "BETWEEN": [utils.testTidFromDate(new Date('2013-07-08 18:43:58-0700')),
                                utils.testTidFromDate(new Date('2013-08-08 18:45:58-0700'))]
                        }
                    }
                }
            }).then(function(response) {
                response = response.body;
                deepEqual(response.items, [{
                    key: 'testing',
                    tid: '28730300-0095-11e3-9234-0123456789ab',
                    latestTid: null,
                    body: null,
                    'content-length': null,
                    'content-location': null,
                    'content-sha256': null,
                    'content-type': null,
                    restrictions: null
                }]);
            });
        });
        it('retrieves static columns', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    proj: ["key", "tid", "latestTid", "body"],
                    attributes: {
                        key: 'test2',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700'))
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'test2');
                deepEqual(response.body.items[0].body, new Buffer("<p>test<p>"));
                deepEqual(response.body.items[0].latestTid,
                utils.testTidFromDate(new Date('2014-01-01 00:00:00')));
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: 'test',
                            tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700'))
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 1);
                deepEqual(response.body.items[0].key, 'test');
                deepEqual(response.body.items[0].tid, utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')));
                deepEqual(response.body.items[0].latestTid,
                utils.testTidFromDate(new Date('2014-01-02 00:00:00')));
            });
        });
        it('retrieves using order by', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'get',
                body: {
                    table: "simple-table",
                    order: {tid: "desc"},
                    attributes: {
                        key: "test"
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.body.items.length, 2);
                deepEqual(response.body.items[0].latestTid,
                utils.testTidFromDate(new Date('2014-01-02 00:00:00')));
                deepEqual(response.body.items[1].latestTid,
                utils.testTidFromDate(new Date('2014-01-02 00:00:00')));
                delete response.body.items[0].latestTid;
                delete response.body.items[1].latestTid;
                deepEqual(response.body.items, [{
                    "key": "test",
                    "tid": utils.testTidFromDate(new Date('2013-08-10 18:43:58-0700')),
                    "body": null,
                    "content-type": null,
                    "content-length": null,
                    "content-sha256": null,
                    "content-location": null,
                    "restrictions": null
                }, {
                    key: 'test',
                    tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                    body: null,
                    'content-type': null,
                    'content-length': null,
                    'content-sha256': null,
                    'content-location': null,
                    restrictions: null,
                }]);
            });
        });

        it('honors request domain', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: "simple-table",
                    attributes: {
                        key: "domain test",
                        tid: utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700'))
                    }
                }
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase1.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "domain test",
                            tid: utils.testTidFromDate(new Date('2014-08-11 18:43:58-0700'))
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "domain test"
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body.items[0].tid,
                    utils.testTidFromDate(new Date('2013-08-11 18:43:58-0700')));
                return router.request({
                    uri: '/restbase1.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: "simple-table",
                        attributes: {
                            key: "domain test"
                        }
                    }
                });
            })
            .then(function(response) {
                deepEqual(response.status, 200);
                deepEqual(response.body.items[0].tid,
                    utils.testTidFromDate(new Date('2014-08-11 18:43:58-0700')));
            });
        });
        it('allows getting TTL for individual rows', function() {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'ttl_get_test',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        body: new Buffer("<p>test<p>"),
                        _ttl: 300
                    }
                }
            })
            .then(function(res) {
                deepEqual(res.status, 201);
                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'ttl_get_test'
                        },
                        withTTL: true
                    }
                });
            })
            .then(function(res) {
                deepEqual(res.status, 200);
                deepEqual(res.body.items.length, 1);
                deepEqual(res.body.items[0]._ttl > 290, true);
            });
        });
    });

    context('Delete', () => {
        it('removes discrete values', () => {
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                method: 'put',
                body: {
                    table: 'simple-table',
                    attributes: {
                        key: 'del-test',
                        tid: utils.testTidFromDate(new Date(1))
                    }
                }
            })
            .then(() =>
                router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'del-test'
                        }
                    }
                })
            )
            .then((res) => {
                deepEqual(res.status, 200);
                deepEqual(res.body.items.length, 1);

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'delete',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'del-test',
                            tid: utils.testTidFromDate(new Date(1))
                        }
                    }
                });
            })
            .then((res) => {
                deepEqual(res.status, 204);

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'del-test'
                        }
                    }
                });
            })
            .then((res) => {
                deepEqual(res.status, 404);
                deepEqual(res.body.items.length, 0);
            });
        });
        it('removes a range of values', () => {
            return P.map(Array.from(new Array(20), (x, i) => i), (sec) =>
                router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'put',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'range-del-test',
                            tid: utils.testTidFromDate(new Date(sec * 1e3))
                        }
                    }
                })
            )
            .then((res) =>
                router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'range-del-test'
                        }
                    }
                })
            )
            .then((res) => {
                deepEqual(res.status, 200);
                deepEqual(res.body.items.length, 20);

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'delete',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'range-del-test',
                            tid: { lt: utils.testTidFromDate(new Date(10 * 1e3)) }
                        }
                    }
                });
            })
            .then((res) => {
                deepEqual(res.status, 204);

                return router.request({
                    uri: '/restbase.cassandra.test.local/sys/table/simple-table/',
                    method: 'get',
                    body: {
                        table: 'simple-table',
                        attributes: {
                            key: 'range-del-test'
                        }
                    }
                });
            })
            .then((res) => {
                deepEqual(res.status, 200);
                deepEqual(res.body.items.length, 10);
                deepEqual(res.body.items[0].tid, utils.testTidFromDate(new Date(19 * 1e3)));
                deepEqual(res.body.items[9].tid, utils.testTidFromDate(new Date(10 * 1e3)));
            });
        });
    });

    context('Drop', function() {
        this.timeout(15000);
        it('drops table', function() {
            return router.request({
                uri: "/restbase.cassandra.test.local/sys/table/simple-table",
                method: "delete",
                body: {}
            })
            .then(function(res) {
                deepEqual(res.status, 204);
                return router.request({
                    uri: "/restbase.cassandra.test.local/sys/table/simple-table",
                    method: "get",
                    body: {}
                });
            })
            .then(function(res) {
                deepEqual(res.status, 500);
            });
        });
    });
});
