'use strict';

/**
 * A thin MongoDB layer that wraps
 * [node-mongodb-native](https://github.com/mongodb/node-mongodb-native) to be
 * used with [persistence](https://github.com/n-fuse/persistence).
 * 
 * @module persistence-mongodb
 */

var mongo = require('mongodb');
var async = require('async');

/**
 * Connects to a MongoDB database.
 * 
 * @param {Object}
 *            [options] a single object for the Server options and the DB
 *            options.
 * @param {String}
 *            options.host defaults to `'localhost'`
 * @param {Int}
 *            options.port defaults to `27017` created: -1}]`
 * @param {String}
 *            options.name name of the database created: -1}]`
 * @param {Function(err,
 *            db)} callback is called when an error occurs or when the database
 *            connection has been established.
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {DB}
 *            callback.db the `DB` instance
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
 *            dbConnection db connection object
 * @property {String} protocol Database protocol
 * 
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/db.html}
 */
exports.DB = function DB(dbConnection) {
  this._db = dbConnection;
}

/**
 * Creates/gets a collection. Additionally, this method ensures the fields given
 * in `indexes` are always indexed.
 * 
 * @param {String}
 *            name collection's name
 * @param {Array}
 *            [indexes] array of indexes passed to `ensureIndex()` eg. `['id', {
 *            created: -1}]`
 * @param {Function(err,
 *            collection)} callback is called when an error occurs or when the
 *            collection is returned.
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Collection}
 *            callback.collection the `Collection` instance
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
      return callback(null, new Collection(coll));

    // ensure all indexes before returning the collection
    // therefore indexes are mapped to the ensureIndex method
    indexes.forEach(function(index) {
      coll.ensureIndex(index, function() {
      });
    });

    callback(null, new Collection(coll));
  });
};

// NOTE:
// Removing collections or databases is not supported and can only be done
// manually via the mongo console.

/**
 * @classdesc Thin wrapper around mongodb's collection type.
 * @constructor
 * 
 * @param {Collection}
 *            collection collection object
 * 
 * @see {@link http://mongodb.github.com/node-mongodb-native/api-generated/collection.html}
 */
function Collection(coll) {
  this._coll = coll;
}

/**
 * Finds all records that match a given query.
 * 
 * @param {Object|String}
 *            query resulting objects must match this query. Consult the
 *            [node-mongodb-native
 *            documentation](http://mongodb.github.com/node-mongodb-native/markdown-docs/queries.html#query-object)
 * @param {String|Int|ObjectId}
 *            [query._id] If specified, MongoDB will search by ID
 * @param {String[]}
 *            [fields] specifies the fields of the resulting objects
 * @param {Object}
 *            [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *            res)} callback is called when an error occurs or when the
 *            record(s) return
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.saved the record, if it has been inserted and `1` if the
 *            record has been updated
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

  // call the query
  if (fields === null)
    this._coll.find(query, options).toArray(callback);
  else
    this._coll.find(query, fields).toArray(callback);
};

/**
 * Finds the first record that matches a given query. Use this method, if you
 * know that there will be only one resulting document. (E.g. when you want to
 * find a result by its `_id`.)
 * 
 * @param {Object|String}
 *            query resulting objects must match this query. Consult the
 *            [node-mongodb-native
 *            documentation](http://mongodb.github.com/node-mongodb-native/markdown-docs/queries.html#query-object)
 * @param {String|Int|ObjectId}
 *            [query._id] If specified, MongoDB will search by ID
 * @param {String[]}
 *            [fields] specifies the fields of the resulting objects
 * @param {Object}
 *            [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *            res)} callback is called when an error occurs or when the
 *            record(s) return
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.saved the record, if it has been inserted and `1` if the
 *            record has been updated
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

  // call the query
  if (fields == null)
    this._coll.findOne(query, options, callback);
  else
    this._coll.findOne(query, fields, options, callback);
};

/**
 * Saves a record. If an object with the same `_id` exists already, this will
 * overwrite it completely.
 * 
 * @param {Object}
 *            record record that shall be saved. This parameter can be an
 *            arbitrary _non-circular_ JS object that contains only primitive
 *            values or arrays and objects, no functions.
 * @param {String|Int|ObjectId}
 *            [record._id] ID that is used by MongoDB. If no ID is specified,
 *            the a default MongoDB ID will be generated.
 * @param {Object}
 *            [options] defines extra logic (sorting options, paging etc.)
 * @param {Function(err,
 *            saved)} callback is called when an error occurs or when the record
 *            has been saved
 * @param {Error*}
 *            callback.err the error, if an error occurred or `null`
 * @param {Object|Int}
 *            callback.saved the record, if it has been inserted and `1` if the
 *            record has been updated
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

  this._coll.save(record, options, callback);
};
