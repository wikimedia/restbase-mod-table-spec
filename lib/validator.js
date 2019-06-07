'use strict';

const validator = {};

const validTypes = ['blob', 'set<blob>',
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
    'json', 'set<json>',
    'long', 'set<long>'
];

const validPredKeys = ['eq', 'lt', 'gt', 'le', 'ge', 'between'];

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
 * @param  {Object} schema  the original complete table schema
 * @param  {Array}  index   indexes array that's being validated
 * @return {Object}         updated indexes array
 */
function validateIndexSchema(schema, index) {
    if (!Array.isArray(index) || !index.length) {
        throw new Error(`Invalid index. Must have at least one entry ${JSON.stringify(index)}`);
    }

    const attrArray = index.map((ind) => ind.attribute);
    if (!checkIfArrayIsUnique(attrArray)) {
        throw new Error('Invalid index. Duplicate index entries.');
    }

    let haveHash = false;

    index.forEach((elem) => {
        if (!schema.attributes[elem.attribute]) {
            throw new Error(`Index element ${JSON.stringify(elem)
            } is not in attributes!`);
        }

        switch (elem.type) {
            case 'hash':
                haveHash = true;
                break;
            case 'range':
                if (elem.order && elem.order !== 'asc' && elem.order !== 'desc') {
                    throw new Error(`Invalid order: ${elem.order}`);
                }
                break;
            case 'static':
                if (!index.some((idx) => idx.type === 'range')) {
                    throw new Error('Cannot create static column in table without range keys');
                }
                break;
            case 'proj':
                break;
            default:
                throw new Error(`Invalid index element encountered! ${JSON.stringify(elem)}`);
        }
    });

    if (!haveHash) {
        throw new Error('Indexes without hash are not yet supported!');
    }

    const order = ['hash', 'static', 'range', 'proj'];
    let sortedIndex = [];
    order.forEach((type) => {
        sortedIndex = sortedIndex.concat(index.filter((idxElem) => idxElem.type === type));
    });

    return sortedIndex;
}

/**
 * Validates the schema options. Throws {Error} if any
 * of the preconditions fail.
 * @param  {Object} schema  original table schema
 * @return {Object}         The original schema, possibly with updated options.
 */
function validateAndNormalizeOptions(schema) {
    schema.options = schema.options || {};
    return schema;
}

/**
 * Validates the table schema. If any of the following precondition fail,
 * {Error} is thrown.
 *
 * - Schema must have at least one attribute
 * - Attribute types must be valid
 * - Indexes and revision policy are also validated
 * @param {Object} schema
 * @return {*}
 */
validator.validateAndNormalizeSchema =  (schema) => {
    if (!schema.version) {
        schema.version = 1;
    }

    if (!schema.attributes) {
        throw new Error('Attributes are required');
    }

    Object.keys(schema.attributes).forEach((attr) => {
        const type = schema.attributes[attr];
        if (validTypes.indexOf(type) < 0) {
            throw new Error(`Invalid type of attribute: ${type}`);
        }
    });

    // Normalize & validate indexes
    schema.index = validateIndexSchema(schema, schema.index);

    // Normalize / validate options
    schema = validateAndNormalizeOptions(schema);

    // XXX: validate attributes
    return schema;
};

/**
 * Validates a get request projection. In case any of the following preconditions fail,
 * {Error} is thrown.
 *
 * - A projection must be a String or Array
 * - All projection attributes must exist in a table/index schema
 * @param {Object} proj     a projection to validate
 * @param {Object} schema   original table schema
 */
function validateProj(proj, schema) {
    proj = proj || schema.proj;
    if (proj) {
        if (!Array.isArray(proj) && proj.constructor !== String) {
            throw new Error(`Invalid query. Projection of type ${proj.constructor} not supported`);
        }
        if (proj.constructor === String) {
            proj = [proj];
        }
        if (proj.some((projElem) => Object.keys(schema.attributes).indexOf(projElem) < 0)) {
            throw new Error(`Invalid query. Projection element not in the schema. Proj: ${proj}`);
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
 * @param {Object}  pred            a predicate object from the query
 * @param {Object}  schema          original table schema
 * @param {boolean} primaryKeyOnly  indicates if it should be verified that each key is a primary
 *                                  key
 */
function validatePredicate(pred, schema, primaryKeyOnly) {
    let nonEqFound;
    Object.keys(pred).forEach((predKey) => {
        const predObj = pred[predKey];
        // Check tht a key is in hash/range index
        if (nonEqFound &&
                predObj.constructor === Object &&
                Object.keys(predObj)[0].toLowerCase() !== 'eq') {
            throw new Error('Invalid query. Found predicate after non-eq predicate');
        }

        if (primaryKeyOnly && (!schema.iKeyMap[predKey] ||
                (schema.iKeyMap[predKey].type !== 'hash' &&
                schema.iKeyMap[predKey].type !== 'range'))) {
            throw new Error(`Invalid query. Attribute ${predKey
            } is not a part of primary key and can't be in condition`);
        }
        if (predObj === undefined) {
            throw new Error(`Invalid query. Attribute ${predKey} is undefined`);
        } else if (predObj.constructor === Object) {
            const predOp = Object.keys(predObj)[0].toLowerCase();
            if (validPredKeys.indexOf(predOp) < 0) {
                throw new Error(`Illegal predicate operator for ${JSON.stringify(predObj)}`);
            }
            if (predOp !== 'eq') {
                if (schema.iKeyMap[predKey].type !== 'range') {
                    throw new Error(
                        `Invalid query. Non-eq conditions allowed only on range columns: ${predKey}`
                    );
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
 * @param {Object}  schema original table schema
 * @param {Object}  order  order part of a get query
 */
function validateOrder(schema, order) {
    Object.keys(order).forEach((key) => {
        const dir = order[key];
        if (dir !== 'asc' && dir !== 'desc') {
            throw new Error(`Invalid sort order ${dir} on key ${key}`);
        }
        const idxElem = schema.iKeyMap[key];
        if (!idxElem || idxElem.type !== 'range') {
            throw new Error(`Cannot order on attribute ${key
            }; needs to be a range index, but is ${idxElem.type}`);
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
 * @param {Object} req     a get request to verify
 * @param {Object} schema  original table schema
 */
validator.validateGetRequest = function (req, schema) {
    if (!schema) {
        throw new Error(`Invalid query. No schema for ${req.table}`);
    }

    if (req.index) {
        throw new Error('Secondary indexes not supported!');
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
 * @param {Object} req    a put request to validate
 * @param {Objext} schema original table schema
 */
validator.validatePutRequest = function (req, schema) {
    if (!schema) {
        throw new Error(`Invalid query. No schema for ${req.table}`);
    }

    const missingColumn = Object.keys(req.attributes)
        .find((attrName) => !schema.attributes[attrName]);
    // _ttl is a special attribute, that might not be present in the schema,
    // but still allowed on the put request to set row TTL
    if (missingColumn) {
        throw new Error(`Unknown attribute ${missingColumn}`);
    }

    schema.iKeys.forEach((key) => {
        if (req.attributes[key] === undefined && key !== schema.tid) {
            throw new Error(`Index attribute ${JSON.stringify(key)} missing in ${
                JSON.stringify(req)}; schema: ${JSON.stringify(schema, null, 2)}`);
        }
    });

    if (req.if) {
        validatePredicate(req.if, schema, false);
    }
};

module.exports = validator;
