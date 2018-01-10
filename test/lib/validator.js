'use strict';

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */
require('core-js/shim');

var deepEqual = require('../utils/test_utils').deepEqual;
var validator = require('../../lib/validator');

function extendSchemaInfo(schema) {
    // Create summary data on the primary data index
    schema.iKeys = schema.index.filter(function(elem) {
        return elem.type === 'hash' || elem.type === 'range';
    })
    .map(function(elem) {
        return elem.attribute;
    });
    schema.iKeyMap = {};
    schema.staticKeyMap = {};
    schema.index.forEach(function(elem) {
        if (elem.type === 'static') {
            schema.staticKeyMap[elem.attribute] = elem;
        } else {
            schema.iKeyMap[elem.attribute] = elem;
        }
    });
    return schema;
}

function test(action, expectedError) {
    var caught;
    try {
        action();
    } catch (e) {
        caught = true;
        deepEqual(expectedError.test(e.message), true);
    }
    if (!caught) {
        throw new Error('Error should be thrown');
    }
}

describe('Unit tests for validation methods', function() {
    var sampleSchema = {
        table: 'simple-table',
        attributes: {
            key: 'string',
            tid: 'timeuuid',
            latestTid: 'timeuuid',
            range: 'string',
            body: 'blob'
        },
        index: [
            {attribute: 'key', type: 'hash'},
            {attribute: 'latestTid', type: 'static'},
            {attribute: 'tid', type: 'range', order: 'desc'},
            {attribute: 'range', type: 'range', order: 'desc'}
        ],
        tid: 'tid'
    };
    sampleSchema = validator.validateAndNormalizeSchema(sampleSchema);
    // Prepare iKeys and iKeyMap for a sample schema
    sampleSchema = extendSchemaInfo(sampleSchema);

    describe('Schema validation', function() {
        it('must have at least one attribute', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test'
                });
            }, /Attributes are required/);
        });
        it('attribute types must be valid', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'not-a-valid-type'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'}
                    ]
                });
            }, /Invalid type of attribute/);
        });
        it('index should be non-empty array', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string'
                    },
                    index: []
                });
            }, /Invalid index\. Must have at least one entry/);
        });
        it('index cannot have duplicate attributes', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'key', type: 'range', order: 'desc'}
                    ]
                });
            }, /Invalid index\. Duplicate index entries/);
        });
        it('index must have at least one hash attribute', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid'
                    },
                    index: [
                        {attribute: 'tid', type: 'range', order: 'desc'}
                    ]
                });
            }, /Indexes without hash are not yet supported/);
        });
        it('all indexed attributes must exist in a schema', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'tid', type: 'range', order: 'desc'},
                        {attribute: 'not-in-schema', type: 'range', order: 'desc'}
                    ]
                });
            }, /Index element/);
        });
        it('range index order must be valid', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'tid', type: 'range', order: 'this-is-not-valid'}
                    ]
                });
            }, /Invalid order/);
        });
        it('static indexes cannot be created on a table with no range indexes', function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'tid', type: 'static'}
                    ]
                });
            }, /Cannot create static column in table without range keys/);
        });
        it('invalid index names not allowed' , function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid',
                        body: 'string'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'tid', type: 'range', order: 'desc'},
                        {attribute: 'body', type: 'this-is-not-valid'}
                    ]
                });
            }, /Invalid index element encountered/);
        });
        it('options.compression must be an array' , function() {
            test(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string',
                        tid: 'timeuuid'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'},
                        {attribute: 'tid', type: 'range', order: 'desc'}
                    ],
                    options: {
                        compression: 'invalid-value'
                    }
                });
            }, /Invalid option value/);
        });
        it('options.updates is filled in' , function() {
            var schema = validator.validateAndNormalizeSchema({
                table: 'test',
                attributes: {
                    key: 'string',
                    tid: 'timeuuid'
                },
                index: [
                    {attribute: 'key', type: 'hash'},
                    {attribute: 'tid', type: 'range', order: 'desc'}
                ],
            });
            deepEqual(schema.options, {
                updates: {
                    pattern: 'random-update',
                }
            });
        });
    });

    describe('PUT request validation', function() {
        it('table schema must exist', function() {
            test(function() {
                validator.validatePutRequest({
                    table: 'test'
                }, null);
            }, /Invalid query\. No schema/);
        });

        it('all index keys must be provided', function() {
            test(function() {
                validator.validatePutRequest({
                    table: 'test',
                    attributes: {
                        body: 'test'
                    }
                }, sampleSchema);
            }, /Index attribute/);
        });

        it('all attributes must exist in schema', function() {
            test(function() {
                validator.validatePutRequest({
                    table: 'test',
                    attributes: {
                        key: 'key',
                        tid: 'timeuuid',
                        latestTid: 'timeuuid',
                        range: 'string',
                        extra_column: 'extra'
                    }
                }, sampleSchema);
            }, /Unknown attribute extra_column/);
        });
    });

    describe('GET request validation', function() {
        it('table schema must exist', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test'
                }, null);
            }, /Invalid query\. No schema/);
        });

        it('all projection attributes must exist', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    proj: [ 'some_random_proj_attr' ]
                }, sampleSchema);
            }, /Invalid query\. Projection /);
        });

        it('every attribute in the predicate must be indexed', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        body: 'sample_body'
                    }
                }, sampleSchema);
            }, /Invalid query\. Attribute /);
        });

        it('every attribute in the predicate must be defined', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        body: undefined
                    }
                }, sampleSchema);
            }, /Invalid query\. Attribute /);
        });

        it('non-eq operators allowed only on "range" indexed columns', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        key: { gt: 'test'}
                    }
                }, sampleSchema);
            }, /Invalid query\. Non\-eq conditions allowed only on range columns/);
        });

        it('can\'t have more than one non-eq predicate for different columns', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        tid: { gt: 10},
                        range: { le: 'a'}
                    }
                }, sampleSchema);
            }, /Invalid query\. Found /);
        });

        it('can\'t be an eq predicate after a non-eq predicate', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        tid: { gt: 10},
                        range: 'a'
                    }
                }, sampleSchema);
            }, /Invalid query\. Found /);
        });

        it('predicate operators must be valid', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        tid: { this_is_a_wrong_operator: 10}
                    }
                }, sampleSchema);
            }, /Illegal predicate operator/);
        });

        it('order must be valid', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    order: {
                        tid: 'this-is-not-valid'
                    }
                }, sampleSchema);
            }, /Invalid sort order/);
        });

        it('order attributes must be in range indexed', function() {
            test(function() {
                validator.validateGetRequest({
                    table: 'test',
                    order: {
                        key: 'asc'
                    }
                }, sampleSchema);
            }, /Cannot order on attribute/);
        });

    });
});
