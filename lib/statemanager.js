'use strict';

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');

var STR_UPDATING = 'updating';
var STR_UPDATED = 'updated';

/**
 * @param {Array} redisEndpoints
 * @param {Object} redisOptions
 * @param {Object} logger
 */
function StateManager(redisEndpoints, redisOptions, logger) {

    if (!redisEndpoints || redisEndpoints.length === 0) {
        throw new Error('no redis engpoints');
    }
    
    this.status = STR_UPDATED;
    this.nodes = redisEndpoints;
    this.redisOptions = redisOptions;
    this.LOG = logger;
    this.shardMap = null;
    this.shardSize = function() {
        if (!this.shardMap) return 0;
        return this.shardMap.size;
    };
    
    this.update();
    
    var updateHandler = setInterval(this.update.bind(this), 5000);
    
    this.close = function() {
        this.LOG.info('state manager: closing ...');
        clearInterval(updateHandler);
        this.LOG.info('state manager: closed');
    };
}
util.inherits(StateManager, EventEmitter);

StateManager.prototype.update = function() {
    
    if (this.status === STR_UPDATING) {
        return;
    }

    this.LOG.debug('updating cluster state');

    this.status = STR_UPDATING;
    
    var self = this;
    
    this.getSlotData(function(err, entryHost, slotData) {
       
       self.status = STR_UPDATED;
       
       if (!!err) {
           self.LOG.error(err);
           return;
       }
       
       self.shardMap = self.extractShards(entryHost, slotData);
       self.emit('changed', self.shardMap);
       self.LOG.debug('cluster state updated');
    });
};

/**
 * @param {Function} next {err, host, slotDefinitions}
 */
StateManager.prototype.getSlotData = function(next) {
    var maxTryTimes = this.nodes.length;
    var found = false;
    var tried = function() {
        maxTryTimes--;
        if (maxTryTimes === 0) {
            next(new Error('no avaliable nodes'));
        }
    };
    
    this.nodes.forEach(function(node, index) {
        if (found) return;
        try {
            var client = redis.createClient(node.port, node.host);
            client.once('error', function(err) {
                tried();
                client.end();
            });
            client.on('ready', function() {
                
                if (found) return;
                
                client.send_command('cluster', ['slots'], function(err, slots) {
                    if (err) {
                        tried();
                    } else if (!found) {
                        found = true;
                        next(null, client.options.host, slots);
                    }
                    client.quit();
                });
            });
        } catch(err) {
            tried();
        }
    });
};

/**
 * @param {String} entryHost
 * @param {Array} slotData
 */
StateManager.prototype.extractShards = function(entryHost, slotData) {
    var self = this;
    var shardMap = new Map();
    slotData.forEach(function(shardData, index) {
        var shard = self.extractShard(entryHost, shardData);
        shardMap.set(`${shard.slotStart}-${shard.slotEnd}`, shard);
    });
    return shardMap;
};

/**
 * 
 */
StateManager.prototype.extractShard = function(entryHost, shardData) {
    var slotStart = +shardData[0];
    var slotEnd = +shardData[1];
    var master = null;
    var slaves = [];
    
    for (var itemIndex = 2; itemIndex < shardData.length; itemIndex++) {
        var item = shardData[itemIndex];
        if (item[0] == '127.0.0.1' || item[0] == 'localhost') {
            item[0] = entryHost;
        }
        
        var endPoint = {host: item[0], port: +item[1]};
        if (itemIndex === 2) {
            master = endPoint;
        } else {
            slaves.push(endPoint);
        }
    }
    
    return new Shard(slotStart, slotEnd, master, slaves);
};

module.exports = StateManager;