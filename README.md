restbase-mod-table-spec
=======================

[RESTBase](https://github.com/wikimedia/restbase) is a caching / storing API
proxy.

This module contains the shared table storage specification, and provides
functional tests against this spec. Those tests are executed against the
[Cassandra](https://github.com/wikimedia/restbase-mod-table-cassandra) and
[SQLite](https://github.com/wikimedia/restbase-mod-table-sqlite) backends.

# JSON table schema example

Example:
```javascript
{
    table: 'example',
    // Attributes are typed key-value pairs
    attributes: {
        name: 'string',
        property: 'string',
        tid: 'timeuuid',
        length: 'int',
        value: 'string'
    },
    // Primary index structure: The order of index components matters. Simple
    // single-level range queries are supported below the hash key level.
    index: [
        { type: 'hash', attribute: 'name' },
        { type: 'range', order: 'asc', attribute: 'property' },
        { type: 'range', order: 'desc', attribute: 'tid' }
    },
    // Optional secondary indexes on the attributes
    secondaryIndexes: {
        by_tid: {
            { type: 'hash', attribute: 'tid' },
            // Primary key attributes are included implicitly
            // Project some additional attributes into the secondary index
            { type: 'proj', attribute: 'length' }
        }
    },
    // Optional policy for retention of obsolete versions (defaults to type all).
    revisionRetentionPolicy: {
        type: 'latest',
        count: 5,
        grace_ttl: 86400
    }
}
```

## Supported types
- `blob`: arbitrary-sized blob; in practice, should be single-digit MB at most
  (at least for Cassandra backend)
- `set<T>`: A set of type T.
- `int`: A 32-bit signed integer.
- `varint`: A variable-length (arbitrary range) integer. Backends support at
  least a 64 bit signed integer. Note that there might be further limitations
  in client platforms; for example, Javascript can only represent 52bits at
  full integer precision in its Number type. Since our server-side
  implementation decodes JSON to doubles, this is also the maximum range the
  we currently support in practice. We might add support for an alternative
  JSON string representation of larger integers in the future.
- `long`: A 64-bit signed long. Javascript only represents 52 bits in its `Number`
   type, so longs should be represented as strings in clients.
- `decimal`: Decimal number.
- `float`: Single-precision (32-bit) floating point number.
- `double`: Double-precision (64-bit) floating point number.
- `boolean`: A boolean.
- `string`: An UTF8 string.
- `timeuuid`: A [version 1 UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_1_.28MAC_address_.26_date-time.29) as a string. Sorted by timestamp.
- `uuid`: A [version 4 UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_.28random.29) as a string.
- `timestamp`: [ISO 8601 timestamp](https://en.wikipedia.org/wiki/ISO_8601) as
  a string.
- `json`: A JSON sub-object (as an embedded object, not a string), which is transparently parsed back to JSON.

## Secondary index consistency
Queries on secondary indexes are eventually consistent by default. While new
entries are inserted along with the data, it is possible that *false
positives* are returned for a short time after the primary request was
acknowledged. We will also support optional strongly consistent secondary
index requests at the cost of cross-checking the index match with the actual
data, at least on some backends.

## Revision retention policies
In an MVCC system, each update of a record results in a new revision.
Depending on the application, these historical versions may or may not be
useful; It may be desirable to limit the number of revisions retained in
order to bound the storage requirements.

The currently supported revision retention policy types are:

### all
When `revisionRetentionPolicy.type` is `all`, all revisions are maintained
indefinitely.

### latest
When `revisionRetentionPolicy.type` is `latest`, the last
`revisionRetentionPolicy.count` records are maintained, any others are
expired in no less than `revisionRetentionPolicy.grace_ttl` seconds.

### interval
When `revisionRetentionPolicy.type` is `interval`, each `revisionRetentionPolicy.count`
items are maintained each `revisionRetentionPolicy.interval` milliseconds.
Removed items expire no less than `revisionRetentionPolicy.grace_ttl` seconds.

### ttl
When `revisionRetentionPolicy.type` is `ttl`, all items are maintained no less than for
`revisionRetentionPolicy.ttl` seconds, and expire after that period.

### latest_hash
When `revisionRetentionPolicy.type` is `latest_hash`, only the latest row per `hash` key
is kept, all the previous are deleted. The order is established by the ordering of the
first `range` key. This retention policy is not supported for tables with secondary indexes
and for tables with no `range` keys other than `tid`.

## Custom TTL
A custom TTL can be set for individual objects on `PUT` requests by providing a special
`_ttl` integer attribute. Its value indicates the amount of time (in seconds) after which
the record will be removed from storage.
 
Please note, that setting custom `_ttl` for individual rows is a dangerous feature, so do
not mix it with `revisionRetentionPolicy` and generally use only if you know what you are doing.

To select a TTL of a row, provide `withTTL: true` key in the query.
 
# Options

The `option` attribute can be used to tune the storage behavior. Example:

```javascript
options: {
  // Indicate a preference of compression algorithms and parameters, most
  // preferred first.
  compression: [{
    algorithm: 'snappy', // 'lz4' (default), 'deflate', 'snappy'
    block_size: 1024, // powers of two 64 .. 1024
  }],
  // Describe the primary update behavior, to allow backends to tune
  // parameters. In Cassandra, this affects the compaction strategy.
  updates: {
    pattern: 'timeseries' // or: 'write-once', 'random-update' (default)
  }
}
```

## Update patterns: `updates`

- `random-update` (default): Robust support for random writes, updates and deletions,
    but might not perform as well as some of the other options.
- `write-once`: Most data is only written once, and there are few deletions.
- `timeseries`: Write order correlates strongly with range index order. A
    typical use case would be timeseries data, with a timestamp or revision id
    as the first range index element.

# Queries
Select the first 50 entries:

```javascript
{
    table: 'example',
    limit: 50
}
```

Limit the query to 'Tom':
```javascript
{
    table: 'example',
    attributes: {
        name: 'Tom'
    },
    limit: 50
}
```

Limit the query to 'Tom', and select properties that are greater than 'a', and
smaller or equal to 'c'. Also, only select the 'value' column:
```javascript
{
    table: 'example',
    attributes: {
        name: 'Tom',
        property: {
            gt: 'a',
            le: 'c'
        }
    },
    // Only select the 'value' column
    proj: ['value']
    limit: 50
}
```

Now, descend down the primary index tree one level further & perform a
range query on the `tid` key:
```javascript
{
    table: 'example',
    attributes: {
        name: 'Tom',
        property: 'foo', // Note: needs to be fixed
        tid: {
            le: '30b68d20-6ba1-11e4-b3d9-550dc866dac4'
        }
    },
    limit: 50
}
```

Finally, perform an index on the `by_tid` secondary index:
```javascript
{
    table: 'example',
    index: 'by_tid',
    attributes: {
        tid: '30b68d20-6ba1-11e4-b3d9-550dc866dac4'
    },
    limit: 50
}
```

As you can see, these queries always select a contiguous slice of indexed
data, which is fairly efficient. The downside is that you can only query what
you indexed for.


## API alternative to consider: REST URLs for GET queries
Due to the tree structure of primary & secondary indexes, simple prefix
equality or range queries pretty naturally map to URLs like
`/example/Tom/foo`, or `/example//by_id/30b68d20-6ba1-11e4-b3d9-550dc866dac4`
for a secondary index query (note the `//` separator). More complex queries
could be supported with query string syntax like
`/example/Tom/foo/?le=30b68d20-6ba1-11e4-b3d9-550dc866dac4&limit=50`.

The current implementation uses the JSON syntax described above exclusively
(as GET or POST requests with a body), but for external APIs the URL-based API
looks very promising. This is not yet implemented, and needs more thinking
though of all the details before we expose a path-based API externally.
