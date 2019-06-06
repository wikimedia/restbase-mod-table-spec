'use strict';

var router = module.parent.router;
var utils = require('../utils/test_utils.js');
var deepEqual = utils.deepEqual;

describe('Multiranged tables', function () {
    this.timeout(15000);

    before(() => router.setup());

    var multirangedSchema = {
        domain: 'restbase.cassandra.test.local',
        table: 'multiRangeTable',
        options: { durability: 'low' },
        attributes: {
            key: 'string',
            tid: 'timeuuid',
            latestTid: 'timeuuid',
            uri: 'string',
            body: 'blob',
            // 'deleted', 'nomove' etc?
            restrictions: 'set<string>'
        },
        index: [
            { attribute: 'key', type: 'hash' },
            { attribute: 'latestTid', type: 'static' },
            { attribute: 'tid', type: 'range', order: 'desc' },
            { attribute: 'uri', type: 'range', order: 'desc' }
        ]
    };

    it('creates table with more than one range key', function () {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
            method: 'put',
            body: multirangedSchema
        })
        .then(function (response) {
            deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
                method: 'get',
                body: {}
            });
        })
        .then(function (response) {
            deepEqual(response.status, 200);
            deepEqual(response.body, multirangedSchema);
        });
    });

    it('inserts a row with more than one range key', function () {
        var testEntity = {
            key: 'testing',
            tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
            latestTid: null,
            uri: 'test',
            body: null,
            restrictions: null
        };
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
            method: 'put',
            body: {
                table: 'multiRangeTable',
                attributes: {
                    key: 'testing',
                    tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    uri: 'test'
                }
            }
        })
        .then(function (response) {
            deepEqual(response, { status: 201 });
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'get',
                body: {
                    table: 'multiRangeTable',
                    attributes: {
                        key: 'testing'
                    }
                }
            });
        })
        .then(function (response) {
            deepEqual(response.status, 200);
            deepEqual(response.body.items.length, 1);
            deepEqual(response.body.items[0], testEntity);
        });
    });

    it('correctly sorts results', function () {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
            method: 'put',
            body: {
                table: 'multiRangeTable',
                attributes: {
                    key: 'sorting',
                    tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    uri: '1'
                }
            }
        })
        .then(function (response) {
            deepEqual(response, { status: 201 });
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'put',
                body: {
                    table: 'multiRangeTable',
                    attributes: {
                        key: 'sorting',
                        tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                        uri: '2'
                    }
                }
            });
        })
        .then(function (response) {
            deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'get',
                body: {
                    table: 'multiRangeTable',
                    attributes: {
                        key: 'sorting'
                    }
                }
            });
        })
        .then(function (response) {
            deepEqual(response.status, 200);
            deepEqual(response.body.items.length, 2);
            deepEqual(response.body.items[0].uri, '2');
            deepEqual(response.body.items[1].uri, '1');
        });
    });

    it('first sorts on first range column', function () {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
            method: 'put',
            body: {
                table: 'multiRangeTable',
                attributes: {
                    key: 'order',
                    tid: utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')),
                    uri: '2'
                }
            }
        })
        .then(function (response) {
            deepEqual(response, { status: 201 });
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'put',
                body: {
                    table: 'multiRangeTable',
                    attributes: {
                        key: 'order',
                        tid: utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')),
                        uri: '1'
                    }
                }
            });
        })
        .then(function (response) {
            deepEqual(response.status, 201);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable/',
                method: 'get',
                body: {
                    table: 'multiRangeTable',
                    attributes: {
                        key: 'order'
                    }
                }
            });
        })
        .then(function (response) {
            deepEqual(response.status, 200);
            deepEqual(response.body.items.length, 2);
            deepEqual(response.body.items[0].uri, '1');
            deepEqual(response.body.items[0].tid, utils.testTidFromDate(new Date('2013-08-09 18:43:58-0700')));
            deepEqual(response.body.items[1].uri, '2');
            deepEqual(response.body.items[1].tid, utils.testTidFromDate(new Date('2013-08-08 18:43:58-0700')));
        });
    });

    it('drops table', function () {
        return router.request({
            uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
            method: 'delete',
            body: {}
        })
        .then(function (res) {
            deepEqual(res.status, 204);
            return router.request({
                uri: '/restbase.cassandra.test.local/sys/table/multiRangeTable',
                method: 'get',
                body: {}
            });
        })
        .then(function (res) {
            deepEqual(res.status, 500);
        });
    });
});
