'use strict';

/**
 * A thin MongoDB layer that wraps
 * [node-mongodb-native](https://github.com/mongodb/node-mongodb-native) to be
 * used with [restposten](https://github.com/n-fuse/restposten).
 *
 * @module restposten-mongodb
 */

var mongo = require('mongodb');
var async = require('async');

/**
 * Connects to a MongoDB database.
 *
 * @param {Object}
 *                [options] a single object for the Server options and the DB
 *                options.
 * @param {String}
 *                options.host defaults to `'localhost'`
 * @param {Int}
 *                options.port defaults to `27017` created: -1}]`
 * @param {String}
 *                options.name name of the database created: -1}]`
 * @param {Function(err,
 *                db)} callback is called when an error occurs or when the
 *                database connection has been established.
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {DB}
 *                callback.db the `DB` instance
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/markdown-docs/database.html#server-options}
 *      and
 *      {@link http://mongodb.github.com/node-mongodb-native/markdown-docs/database.html#db-options}
 *      for more information on the possible options
 */
exports.connect = function(options, callback) {
  // set optional arguments
  if (arguments.length == 1) {
    callback == options;
    options = {};
  }

  var host = options.host || 'localhost';
  var port = options.port || 27017;

  // strict mode must always be off, since otherwise, collection creation etc.
  // would not work
  options.strict = false;
  options.safe = true;

  var mongoServer = new mongo.Server(host, port, options);
  var dbConnector = new mongo.Db(options.name, mongoServer, options);

  // open database but only return an object that only has a few methods
  dbConnector.open(function(err, conn) {
    if (err)
      return callback(err);

    callback(null, new DB(conn));
  });
};

/**
 * @classdesc Thin wrapper around mongodb's Db class.
 * @constructor
 *
 * @param {Db}
 *                dbConnection db connection object
 * @property {String} protocol Database protocol
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/db.html}
 */
function DB(dbConnection) {
  this._db = dbConnection;
}

/**
 * Creates/gets a collection. Additionally, this method ensures the fields given
 * in `indexes` are always indexed.
 *
 * @param {String}
 *                name collection's name
 * @param {Array}
 *                [indexes] array of indexes passed to `ensureIndex()` eg.
 *                `['id', { created: -1}]`
 * @param {Function(err,
 *                collection)} callback is called when an error occurs or when
 *                the collection is returned.
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Collection}
 *                callback.collection the `Collection` instance
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/markdown-docs/indexes.html}
 */
DB.prototype.getCollection = function(name, indexes, callback) {
  if (arguments.length == 2) {
    callback = indexes;
    indexes = null;
  }

  this._db.collection(name, function(err, coll) {
    if (err)
      return callback(err);

    // when there are no indexes, return the collection immediately
    if (indexes === null || indexes.length == 0)
      return callback(null, new Collection(name, coll));

    // ensure all indexes before returning the collection
    // therefore indexes are mapped to the ensureIndex method
    indexes.forEach(function(index) {
      coll.ensureIndex(index, function() {
      });
    });

    callback(null, new Collection(name, coll));
  });
};

/**
 * Closes the connection to the database.
 *
 * @param {Boolean}
 *                [force] force to close the connect so that it cannot be
 *                reused.
 * @param {Function(err,
 *                results)} callback is called when the db has been closed or an
 *                error occurred
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Object}
 *                callback.results
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/db.html#close}
 */
DB.prototype.close = function(force, callback) {
  if (arguments.length == 1)
    this._db.close(arguments[0]);
  else
    this._db.close(force, callback);
};

// NOTE:
// Removing whole collections or databases is not supported and can only be done
// manually via the mongo console.

/**
 * @classdesc Thin wrapper around mongodb's collection type.
 * @constructor
 *
 * @param {String}
 *                name name of the collection
 * @param {Object}
 *                collection collection object
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html}
 *
 * @property {String} name name of the collection
 */
function Collection(name, coll) {
  this._coll = coll;

  this.__defineGetter__('name', function() {
    return name;
  });
}

/**
 * @param query
 *
 * @private
 */
function fixID(query) {
  // if the query _id is a hex string that is 24 digits long, convert to
  // ObjectID
  if (typeof query._id == 'string' && /^[0-9A-F]{24}$/i.test(query._id))
    query._id = mongo.ObjectID.createFromHexString(query._id);
}

/**
 * Finds all records that match a given query.
 *
 * @param {Object|String}
 *                query resulting objects must match this query. Consult the
 *                [node-mongodb-native
 *                documentation](http://mongodb.github.com/node-mongodb-native/markdown-docs/queries.html#query-object)
 * @param {String|Int|ObjectId}
 *                [query._id] If specified, MongoDB will search by ID
 * @param {String[]}
 *                [fields] specifies the fields of the resulting objects
 * @param {Object}
 *                [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *                res)} callback is called when an error occurs or when the
 *                record(s) return
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *                callback.saved the record, if it has been inserted and `1` if
 *                the record has been updated
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#find}
 */
Collection.prototype.find = function(query, fields, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = fields;
    fields = null;
    options = {};
  } else if (arguments.length == 3) {
    callback = options;
    options = fields;
    fields = null;
  }

  fixID(query);

  // call the query
  if (fields === null)
    this._coll.find(query, options).toArray(callback);
  else
    this._coll.find(query, fields, options).toArray(callback);
};

/**
 * Finds the first record that matches a given query. Use this method, if you
 * know that there will be only one resulting document. (E.g. when you want to
 * find a result by its `_id`.)
 *
 * @param {Object|String}
 *                query resulting objects must match this query. Consult the
 *                [node-mongodb-native
 *                documentation](http://mongodb.github.com/node-mongodb-native/markdown-docs/queries.html#query-object)
 * @param {String|Int|ObjectId}
 *                [query._id] If specified, MongoDB will search by ID
 * @param {String[]}
 *                [fields] specifies the fields of the resulting objects
 * @param {Object}
 *                [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *                res)} callback is called when an error occurs or when the
 *                record(s) return
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *                callback.saved the record, if it has been inserted and `1` if
 *                the record has been updated
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#find}
 */
Collection.prototype.findOne = function(query, fields, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = fields;
    fields = null;
    options = {};
  } else if (arguments.length == 3) {
    callback = options;
    options = fields;
    fields = null;
  }

  fixID(query);

  // call the query
  if (fields === null)
    this._coll.findOne(query, options, callback);
  else
    this._coll.findOne(query, fields, options, callback);
};

/**
 * Saves a record. If an object with the same `_id` exists already, this will
 * overwrite it completely.
 *
 * @param {Object}
 *                record record that shall be saved. This parameter can be an
 *                arbitrary _non-circular_ JS object that contains only
 *                primitive values or arrays and objects, no functions.
 * @param {String|Int|ObjectId}
 *                [record._id] ID that is used by MongoDB. If no ID is
 *                specified, the a default MongoDB ID will be generated.
 * @param {Object}
 *                [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *                saved)} callback is called when an error occurs or when the
 *                record has been saved
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *                callback.saved the record, if it has been inserted and `1` if
 *                the record has been updated
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#save}
 */
Collection.prototype.save = function(record, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = options;
    options = {};
  }

  options.safe = true;
  fixID(record);

  this._coll.save(record, options, callback);
};

/**
 * Replaces a record. If there already is a record that matches the criteria,
 * this will overwrite it completely.
 *
 * @param {Object}
 *                criteria criteria for the record to be replaced.
 * @param {Object}
 *                record record that shall be saved. This parameter can be an
 *                arbitrary _non-circular_ JS object that contains only
 *                primitive values or arrays and objects, no functions.
 * @param {Object}
 *                [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,saved)}
 *                callback is called when an error occurs or when the record has
 *                been saved
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *                callback.saved the record, if it has been inserted and `1` if
 *                the record has been updated
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#update}
 */
Collection.prototype.update = function(criteria, record, options, callback) {
  if (arguments.length == 3) {
    callback = options;
    options = {};
  }

  options.safe = true;
  fixID(record);

  this._coll.update(criteria, record, options, callback);
};

/**
 * Deletes all records that match the query.
 *
 * @param {Object}
 *                query Query object
 * @param {Object}
 *                [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,deleted)}
 *                callback is called when an error occurs or when the record has
 *                been saved
 * @param {Error*}
 *                callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *                callback.deleted the record, if it has been inserted and `1`
 *                if the record has been updated
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html#delete}
 */
Collection.prototype.delete = function(query, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = options;
    options = {};
  }

  options.safe = true;
  fixID(query);

  this._coll.remove(query, options, callback);
};

/**
 * Count all records in a collection that match the query.
 *
 * @param {Object}
 *                [query]
 * @param {Object}
 *                [options]
 * @param {Function(err,count)}
 *                callback
 *
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collectionhtml#count}
 */
Collection.prototype.count = function(query, options, callback) {
  // optional arguments
  if (arguments.length == 2) {
    callback = options;
    options = {};
  } else if (arguments.length == 1) {
    callback = options;
    options = {};
  }

  options.safe = true;
  fixID(query);

  this._coll.count(query, options, callback);
};
