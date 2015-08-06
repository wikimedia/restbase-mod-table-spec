"use strict";

var validator = {};

var validCompressionAlgorithms = {
    lz4: 'LZ4Compressor',
    deflate: 'DeflateCompressor',
    lzma: false,
    snappy: 'SnappyCompressor'
};
var validCompressionBlockSizes = {
    64: 1,
    128: 1,
    256: 1,
    512: 1,
    1024: 1
};
var validTypes = ['blob', 'set<blob>',
    'decimal', 'set<decimal>',
    'double', 'set<double>',
    'float', 'set<float>',
    'boolean', 'set<boolean>',
    'int', 'set<int>',
    'varint', 'set<varint>',
    'string', 'set<string>',
    'timeuuid', 'set<timeuuid>',
    'uuid', 'set<uuid>',
    'timestamp', 'set<timestamp>',
    'json', 'set<json>'
];

var validPredKeys = ['eq', 'lt', 'gt', 'le', 'ge', 'between'];

function checkIfArrayIsUnique(arr) {
    return new Set(arr).size === arr.length;
}

/**
 * Validates index schemas array and throws {Error} if some preconditions are failed.
 *
 * The following preconditions are validated:
 * - Index should be non-empty array
 * - Index cannot have duplicate attributes
 * - Index must have at least one 'hash' attribute
 * - All indexed attributes must exist in a schema
 * - Static indexes cannot be created on a table with no range indexes
 *
 * @param schema the original complete table schema
 * @param index indexes array that's being validated
 * @returns {Object} updated indexes array
 */
function validateIndexSchema(schema, index) {
    if (!Array.isArray(index) || !index.length) {
        throw new Error("Invalid index " + JSON.stringify(index));
    }

    var attrArray = index.map(function(ind) {
        return ind.attribute;
    });
    if (!checkIfArrayIsUnique(attrArray)) {
        throw new Error("Invalid index. Duplicate index entries.");
    }

    var haveHash = false;

    index.forEach(function(elem) {
        if (!schema.attributes[elem.attribute]) {
            throw new Error('Index element ' + JSON.stringify(elem)
                + ' is not in attributes!');
        }

        switch (elem.type) {
            case 'hash':
                haveHash = true;
                break;
            case 'range':
                if (elem.order !== 'asc' && elem.order !== 'desc') {
                    // Default to ascending sorting.
                    //
                    // Normally you should specify the sorting explicitly. In
                    // particular, you probably always want to use descending
                    // order for time series data (timeuuid) where access to the
                    // most recent data is most common.
                    elem.order = 'desc';
                }
                break;
            case 'static':

                if (!index.some(function(idx) {
                    return idx.type === 'range';
                })) {
                    throw new Error('Cannot create static column in table without range keys');
                }
                break;
            case 'proj':
                break;
            default:
                throw new Error('Invalid index element encountered! ' + JSON.stringify(elem));
        }
    });

    if (!haveHash) {
        throw new Error("Indexes without hash are not yet supported!");
    }

    return index;
}

/**
 * Validates the revision policy schema. Throws {Error} if any
 * of the preconditions fail.
 *
 * The following preconditions are validated:
 * - Only keys 'type', 'grace_ttl' and 'count' are allowed
 * - Type can be 'all' and 'latest'
 * - Grace_ttl must be a number and can't be less then minGcGrace
 * - Count must be a number and must be between minKeep and maxKeep
 *
 * @param schema original table schema
 */
function validateAndNormalizeRevPolicy(schema) {
    // FIXME: define as constants somewhere apropos
    var minGcGrace = 10;
    var minKeep = 1;
    var maxKeep = 1000000000;

    var policy;

    if (schema.revisionRetentionPolicy) {
        policy = schema.revisionRetentionPolicy;
        Object.keys(policy).forEach(function(key) {
            var val = policy[key];
            switch(key) {
                case 'type':
                    if (val !== 'all' && val !== 'latest') {
                        throw new Error('Invalid revision retention policy type '+val);
                    }
                    break;
                case 'grace_ttl':
                    if (typeof(val) !== 'number') {
                        throw new Error('grace_ttl must be a number');
                    }
                    if (val < minGcGrace) {
                        throw new Error('grace_ttl must be a miniumum of '+minGcGrace+' seconds');
                    }
                    policy.grace_ttl = val;
                    break;
                case 'count':
                    if (typeof(val) !== 'number') {
                        throw new Error('count must be a number');
                    }
                    if ((val < minKeep) || (val > maxKeep)) {
                        throw new Error('count must be a value between '+minKeep+' and '+maxKeep);
                    }
                    policy.count = val;
                    break;
                default:
                    throw new Error('Unknown revision policy attribute: ' + key);
            }
        });
    }

    return policy;
}

/**
 * Validates the table schema. If any of the following precondition fail,
 * {Error} is thrown.
 *
 * - Schema must have at least one attribute
 * - Attribute types must be valid
 * - Compression option must have valid algorithm and block size
 * - Durablity option must have valid value
 * - Indexes and revision policy are also validated
 *
 * @param schema
 * @returns {*}
 */
validator.validateAndNormalizeSchema = function(schema) {
    if (!schema.version) {
        schema.version = 1;
    }

    if (!schema.attributes) {
        throw new Error('Attributes are required');
    }

    Object.keys(schema.attributes).forEach(function(attr) {
        var type = schema.attributes[attr];
        if (validTypes.indexOf(type) < 0) {
            throw new Error('Invalid type of attribute: ' + type);
        }
    });

    // Check options
    if (schema.options) {
        Object.keys(schema.options).forEach(function(key) {
            var val = schema.options[key];
            switch(key) {
                case 'compression':
                    if (!Array.isArray(val)
                    || !val.length
                    || val.some(function(algo) {
                        var cassAlgo = validCompressionAlgorithms[algo.algorithm];
                        var cassBlockSize = validCompressionBlockSizes[algo.block_size];
                        return cassAlgo === undefined || cassAlgo === false
                                    || cassBlockSize === undefined || cassBlockSize === false;
                    })) {
                        throw new Error('Invalid compression settings: '
                        + JSON.stringify(val));
                    }
                    break;
                case 'durability':
                    if (val !== 'low' && val !== 'standard') {
                        throw new Error ('Invalid durability level: ' + val);
                    }
                    break;
                default:
                    throw new Error('Unknown option: ' + key);
            }
        });
    }

    // Normalize & validate indexes
    schema.index = validateIndexSchema(schema, schema.index);
    schema.secondaryIndexes = schema.secondaryIndexes || {};
    Object.keys(schema.secondaryIndexes).forEach(function(index) {
        schema.secondaryIndexes[index] = validateIndexSchema(schema, schema.secondaryIndexes[index]);
    });

    // Normalize and validate revision retention policy
    var policy = validateAndNormalizeRevPolicy(schema);
    if (policy) {
        schema.revisionRetentionPolicy = policy;
    }

    // XXX: validate attributes
    return schema;
};

/**
 * Validates a get request projection. In case any of the following preconditions fail,
 * {Error} is thrown.
 *
 * - A projection must be a String or Array
 * - All projection attributes must exist in a table/index schema
 * @param proj a projection to validate
 * @param schema original table schema
 */
function validateProj(proj, schema) {
    proj = proj || schema.proj;
    if (!Array.isArray(proj) && !proj.constructor === String) {
        throw new Error('Invalid query. Projection of type ' + proj.constructor + ' not supported');
    }
    if (proj.constructor === String) {
        proj = [ proj ];
    }
    if (proj.some(function(projElem) {
        return Object.keys(schema.attributes).indexOf(projElem) < 0;
    })) {
        throw new Error('Invalid query. Projection element ' + projElem + ' not in the schema');
    }
}

/**
 * Validates a predicate option of a get query. If any of the following preconditions
 * fail, {Error} is thrown.
 *
 * - If primaryKeyOnly is true, every attribute in the predicate must be 'hash' or 'range' indexed
 * - Each individual condition operator must be valid
 * - Non-eq operators allowed only on 'range' indexed columns
 * - There can't be more than one non-eq predicates.
 *
 * @param pred a predicate object from the query
 * @param schema original table schema
 * @param primaryKeyOnly indicates if it should be verified that each key is a primary key
 */
function validatePredicate(pred, schema, primaryKeyOnly) {
    var nonEqFound;
    Object.keys(pred).forEach(function(predKey) {
        var predObj = pred[predKey];
        // Check tht a key is in hash/range index
        if (primaryKeyOnly && (!schema.iKeyMap[predKey] ||
                (schema.iKeyMap[predKey].type !== 'hash'
                && schema.iKeyMap[predKey].type !== 'range'))) {
            throw new Error('Invalid query. attribute ' + predKey
                + " is not a part of primary key and can't be in condition")
        }
        if (predObj === undefined) {
            throw new Error('Invalid query. attribute ' + predKey + ' is undefined');
        } else if (predObj.constructor === Object) {
            var predOp = Object.keys(predObj)[0].toLowerCase();
            if (validPredKeys.indexOf(predOp) < 0) {
                throw new Error('Illegal predicate operator for ' + JSON.stringify(predObj));
            }
            if (predOp !== 'eq') {
                if (schema.iKeyMap[predKey].type !== 'range') {
                    throw new Error('Invalid query. Non-eq conditions allowed only on range columns: ' + predKey);
                }
                if (nonEqFound !== undefined) {
                    throw new Error('Invalid query. Found ' + predOp + ' after ' +  nonEqFound);
                }
                nonEqFound = predOp;
            }
        }
    });
}

/**
 * Validates the order of a get request. If any of the following preconditions fail,
 * {Error} is thrown.
 *
 * - Order must be either 'asc' or 'desc'
 * - Order attributes must be in a 'range' index of a table
 *
 * @param schema original table schema
 * @param order order part of a get query
 */
function validateOrder(schema, order) {
    Object.keys(order).forEach(function(key) {
        var dir = order[key];
        if (dir !== 'asc' && dir !== 'desc') {
            throw new Error("Invalid sort order " + dir + " on key " + key);
        }
        var idxElem = schema.iKeyMap[key];
        if (!idxElem || idxElem.type !== 'range') {
            throw new Error("Cannot order on attribute " + key
                + "; needs to be a range index, but is " + idxElem.type);
        }
    });
}

/**
 * Validates a get request. In case any of the following preconditions fail,
 * {Error} is thrown.
 *
 * - Table schema must exist
 * - If the query uses a secondary index, it must exist
 * - Projection, condition and order parts are also verified
 *
 * @param req a get request to verify
 * @param schema original table schema
 */
validator.validateGetRequest = function(req, schema) {
    if (!schema) {
        throw new Error('Invalid query. No schema for ' + req.table);
    }

    if (req.index) {
        schema = schema.secondaryIndexes[req.index];
        if (!schema) {
            throw new Error('Inalid query. Index does not exist: ' + req.index);
        }
    }
    validateProj(req.proj, schema);
    if (req.attributes) {
        validatePredicate(req.attributes, schema, true);
    }
    if (req.order) {
        validateOrder(schema, req.order);
    }
};

/**
 * Validates a put request. If any of the following preconditions fail,
 * {Error} is thrown
 *
 * - A table schema must exist
 * - All primary key attributes must be provided
 *
 * @param req a put request to validate
 * @param schema original table schema
 */
validator.validatePutRequest = function(req, schema) {
    if (!schema) {
        throw new Error('Invalid query. No schema for ' + req.table);
    }

    schema.iKeys.forEach(function(key) {
        if (req.attributes[key] === undefined && key !== schema.tid) {
            throw new Error("Index attribute " + JSON.stringify(key) + " missing in "
                + JSON.stringify(req) + "; schema: " + JSON.stringify(schema, null, 2));
        }
    });

    if (req.if) {
        validatePredicate(req.if, schema, false);
    }
};

module.exports = validator;
