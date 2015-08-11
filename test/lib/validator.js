'use strict';

// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var deepEqual = require('../utils/test_utils').deepEqual;
var validator = require('../../lib/validator');
var P = require('bluebird');

function extendSchemaInfo(schema) {
    // Create summary data on the primary data index
    schema.iKeys = schema.index.filter(function(elem) {
        return elem.type === 'hash' || elem.type === 'range'
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
            return P.try(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test'
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Attributes are required/.test(e.message), true);
            });
        });
        it('attribute types must be valid', function() {
            return P.try(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'not-a-valid-type'
                    },
                    index: [
                        {attribute: 'key', type: 'hash'}
                    ]
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid type of attribute/.test(e.message), true);
            });
        });
        it('index should be non-empty array', function() {
            return P.try(function() {
                validator.validateAndNormalizeSchema({
                    table: 'test',
                    attributes: {
                        key: 'string'
                    },
                    index: []
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid index\. Must have at least one entry/.test(e.message), true);
            });
        });
        it('index cannot have duplicate attributes', function() {
            return P.try(function() {
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
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid index\. Duplicate index entries/.test(e.message), true);
            });
        });
        it('index must have at least one hash attribute', function() {
            return P.try(function() {
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
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Indexes without hash are not yet supported/.test(e.message), true);
            });
        });
        it('all indexed attributes must exist in a schema', function() {
            return P.try(function() {
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
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Index element/.test(e.message), true);
            });
        });
        it('range index order must be valid', function() {
            return P.try(function() {
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
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid order/.test(e.message), true);
            });
        });
        it('static indexes cannot be created on a table with no range indexes', function() {
            return P.try(function() {
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
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Cannot create static column in table without range keys/.test(e.message), true);
            });
        });
        it('invalid index names not allowed' , function() {
            return P.try(function() {
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
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid index element encountered/.test(e.message), true);
            });
        });
        it('revision policy must have valid keys' , function() {
            return P.try(function() {
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
                    revisionRetentionPolicy: {
                        'invalid-key': 'invalid-value'
                    }
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Unknown revision policy attribute/.test(e.message), true);
            });
        });
        it('revision policy type must be valid' , function() {
            return P.try(function() {
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
                    revisionRetentionPolicy: {
                        type: 'invalid-value'
                    }
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid revision retention policy type/.test(e.message), true);
            });
        });
        it('revision policy grace_ttl must be valid' , function() {
            return P.try(function() {
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
                    revisionRetentionPolicy: {
                        type: 'latest',
                        grace_ttl: 'must not me a string'
                    }
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/grace_ttl must be a number/.test(e.message), true);
            });
        });
        it('revision policy count must be valid' , function() {
            return P.try(function() {
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
                    revisionRetentionPolicy: {
                        type: 'latest',
                        count: 'must not me a string'
                    }
                });
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/count must be a number/.test(e.message), true);
            });
        });
    });

    describe('PUT request validation', function() {
        it('table schema must exist', function() {
            return P.try(function() {
                validator.validatePutRequest({
                    table: 'test'
                }, null)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. No schema/.test(e.message), true);
            });
        });

        it('all index keys must be provided', function() {
            return P.try(function() {
                validator.validatePutRequest({
                    table: 'test',
                    attributes: {
                        body: 'test'
                    }
                }, sampleSchema);
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Index attribute/.test(e.message), true);
            });
        });
    });

    describe('GET request validation', function() {
        it('table schema must exist', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test'
                }, null)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. No schema/.test(e.message), true);
            });
        });

        it('all projection attributes must exist', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    proj: [ 'some_random_proj_attr' ]
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Projection /.test(e.message), true);
            });
        });

        it('every attribute in the predicate must be indexed', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        body: 'sample_body'
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Attribute /.test(e.message), true);
            });
        });

        it('every attribute in the predicate must be defined', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        body: undefined
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Attribute /.test(e.message), true);
            });
        });

        it('non-eq operators allowed only on "range" indexed columns', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        key: { gt: 'test'}
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Non\-eq conditions allowed only on range columns/.test(e.message), true);
            });
        });

        it('can\'t have more than one non-eq predicate for different columns', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        tid: { gt: 10},
                        range: { le: 'a'}
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Found /.test(e.message), true);
            });
        });

        it('can\'t be an eq predicate after a non-eq predicate', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        tid: { gt: 10},
                        range: 'a'
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Found /.test(e.message), true);
            });
        });

        it('predicate operators must be valid', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    attributes: {
                        tid: { this_is_a_wrong_operator: 10}
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Illegal predicate operator/.test(e.message), true);
            });
        });

        it('order must be valid', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    order: {
                        tid: 'this-is-not-valid'
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid sort order/.test(e.message), true);
            });
        });

        it('order attributes must be in range indexed', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    order: {
                        key: 'asc'
                    }
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Cannot order on attribute/.test(e.message), true);
            });
        });

        it('secondart index must be defined', function() {
            return P.try(function() {
                validator.validateGetRequest({
                    table: 'test',
                    index: 'this_does_not_exist'
                }, sampleSchema)
            })
            .then(function() {
                throw new Error('Should throw validation error')
            }, function(e) {
                deepEqual(/Invalid query\. Index does not exist/.test(e.message), true);
            });
        });
    });
});
