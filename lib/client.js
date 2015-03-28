'use strict';

var RedisPool = require('./pool');

function ClusterClient(addressArray, options) {

}

ClusterClient.prototype.get = function(key, args, done) {

};

ClusterClient.prototype.set = function(key, args, done) {

};

ClusterClient.prototype.subscribe = function(channel, done) {

};

ClusterClient.prototype.publish = function(channel, message, done) {

};

module.exports = ClusterClient;