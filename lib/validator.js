"use strict";

var validator = {};

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
        throw new Error("Invalid index. Must have at least one entry " + JSON.stringify(index));
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
                if (elem.order && elem.order !== 'asc' && elem.order !== 'desc') {
                    throw new Error('Invalid order: ' + elem.order);
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

    var order = ['hash', 'static', 'range', 'proj'];
    var sortedIndex = [];
    order.forEach(function(type) {
        sortedIndex = sortedIndex.concat(index.filter(function(idxElem) {
            return idxElem.type === type;
        }));
    });

    return sortedIndex;
}

/**
 * Validates the revision policy schema. Throws {Error} if any
 * of the preconditions fail.
 *
 * The following preconditions are validated:
 * - Only keys 'type', 'interval', 'grace_ttl' and 'count' are allowed
 * - Type can be 'all', 'latest' and 'interval'
 *
 * @param schema original table schema
 */
function validateAndNormalizeRevPolicy(schema) {
    var policy;

    if (schema.revisionRetentionPolicy) {
        policy = schema.revisionRetentionPolicy;
        // Rewrite the `ttl` policy to latest with count = 0
        if (policy.type === 'ttl') {
            policy.type = 'latest';
            policy.count = 0;
            if (typeof(policy.ttl) !== 'number') {
                throw new Error('ttl must be a number');
            }
            policy.grace_ttl = policy.ttl;
            delete policy.ttl;
        }

        Object.keys(policy).forEach(function(key) {
            var val = policy[key];
            switch(key) {
                case 'type':
                    if (val !== 'all' && val !== 'latest' && val !== 'interval') {
                        throw new Error('Invalid revision retention policy type ' + val);
                    }
                    break;
                case 'grace_ttl':
                    if (typeof(val) !== 'number') {
                        throw new Error('grace_ttl must be a number');
                    }
                    policy.grace_ttl = val;
                    break;
                case 'count':
                    if (typeof(val) !== 'number') {
                        throw new Error('count must be a number');
                    }
                    policy.count = val;
                    break;
                case 'interval':
                    if (typeof(val) !== 'number') {
                        throw new Error('count must be a number');
                    }
                    policy.interval = val;
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
    if (proj) {
        if (!Array.isArray(proj) && proj.constructor !== String) {
            throw new Error('Invalid query. Projection of type ' + proj.constructor + ' not supported');
        }
        if (proj.constructor === String) {
            proj = [proj];
        }
        if (proj.some(function(projElem) {
            return Object.keys(schema.attributes).indexOf(projElem) < 0;
        })) {
            throw new Error('Invalid query. Projection element not in the schema. Proj: ' + proj);
        }
    }
}

/**
 * Validates a predicate option of a get query. If any of the following preconditions
 * fail, {Error} is thrown.
 *
 * - If primaryKeyOnly is true, every attribute in the predicate must be 'hash' or 'range' indexed
 * - Each individual condition operator must be valid
 * - Non-eq operators allowed only on 'range' indexed columns
 * - There can't be more than one non-eq predicate for different columns
 * - There can't be an eq predicate after a non-eq predicate
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
        if (nonEqFound) {
            throw new Error('Invalid query. Found predicate after non-eq predicate');
        }

        if (primaryKeyOnly && (!schema.iKeyMap[predKey] ||
                (schema.iKeyMap[predKey].type !== 'hash'
                && schema.iKeyMap[predKey].type !== 'range'))) {
            throw new Error('Invalid query. Attribute ' + predKey
                + " is not a part of primary key and can't be in condition");
        }
        if (predObj === undefined) {
            throw new Error('Invalid query. Attribute ' + predKey + ' is undefined');
        } else if (predObj.constructor === Object) {
            var predOp = Object.keys(predObj)[0].toLowerCase();
            if (validPredKeys.indexOf(predOp) < 0) {
                throw new Error('Illegal predicate operator for ' + JSON.stringify(predObj));
            }
            if (predOp !== 'eq') {
                if (schema.iKeyMap[predKey].type !== 'range') {
                    throw new Error('Invalid query. Non-eq conditions allowed only on range columns: ' + predKey);
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
            throw new Error('Invalid query. Index does not exist: ' + req.index);
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

    var missingColumn = Object.keys(req.attributes).find(function(attrName) {
        return !schema.attributes[attrName];
    });
    if (missingColumn) {
        throw new Error('Unknown attribute ' + missingColumn);
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
