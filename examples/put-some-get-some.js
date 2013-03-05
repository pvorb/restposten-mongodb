var mongo = require('../');
var async = require('async');

async.waterfall([ function(callback) {
  mongo.connect({
    name : 'persistence_test'
  }, callback)
}, function(db, callback) {
  db.getCollection('author', [ '_id' ], callback);
}, function(collection, callback) {
  collection.save({
    name : 'pvorb',
  }, function(err, saved) {
    if (err)
      return callback(err);

    console.log('saved', saved);

    callback(null, collection);
  });
}, function(collection, callback) {
  collection.find({
    name : 'pvorb'
  }, callback);
} ], function(err, records) {
  if (err)
    throw err;

  records.forEach(function(record) {
    console.log('found', record)
  });
});
