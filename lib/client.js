'use strict';
/* jshint loopfunc:true */

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');
var consolelogger = require('./consolelogger');
var commands = require('./commands');
var StateManager = require('./statemanager');
var shardProxy = require('./shardproxy');
var Transaction = require('./transaction');
var Pipelining = require('./pipelining');
var PubSub = require('./pubsub');

/**
 *
 *
 * @param {Array} redisEndpoints [{host: string, port: number}]
 * @param {Object} redisOptions
 * @param {Number} slaveMode
 *  0: read/write master
 *  1: read from slave when master is unavailable
 *  2: write to master, read from slaves
 * @param {Object} logger
 * @constructor
 */
function ClusterClient(redisEndpoints, redisOptions, slaveMode, logger) {
    var _self = this;
    var _queuedCallbacks = [];
    var _maxQueueSize = 100;

    this.LOG = logger || consolelogger;
    
    this.keySlotMap = new Map(); // Useful for testing. Maintain key-slot mapping manually.
    this.ready = false;
    this.status = 'initializing';
    this.redisNodes = redisEndpoints;
    this.redisOptions = redisOptions || {};
    this.slaveMode = slaveMode || 0;
    this.shardProxies = new shardProxy.ProxyList();
    this.commands = commands;
    
    /**
     * Push a callback function into queue.
     */
    this.enqueue = function(callback) {
        if (_queuedCallbacks.length > _maxQueueSize) {
            return false;
        }
        
        _queuedCallbacks.push(callback);
        return true;
    };
    
    /**
     * Pop and returns a callback function from queue.
     * @returns {Function} a callback function
     */
    this.dequeue = function() {
        return _queuedCallbacks.pop();
    };
    
    this.stateManager = new StateManager(this.redisNodes, this.redisOptions, this.LOG);
    this.stateManager.on('changed', function(shardMap) {
        _self.refreshShardProxies(shardMap, function() {
            _self.pubsub.setServerGetter(
                _self.redisOptions,
                function() {
                    return _self.getProxy('pubsub').getMaster();
                });
            if(!_self.ready) {
                _self.on_ready();
            }
        });
    });
    
    /**
     * Returns a proxy instance for input key(s). 
     * If do not given any key, return the first proxy in list.
     * If keys belongs to different slot, will raise an error.
     * 
     * @param {Array} keys
     * @returns {ShardProxy}
     */
    this.getProxy = function(keys) {
        // do not have key for command
        if(!keys || (Array.isArray(keys) && keys.length === 0)) {
            return this.shardProxies.getFirst();
        }
        
        if(!Array.isArray(keys)) {
            keys = [keys];
        }

        var _slot = null;

        for(var k=0; k<keys.length; k++) {

            var tmpSlot = this.keySlotMap.has(keys[k]) ? this.keySlotMap.get(keys[k]) : shardProxy.calculateSlot(keys[k]);

            if(_slot === null) {
                _slot = tmpSlot;
            } else if(_slot != tmpSlot) {
                throw new Error('Keys are in different slots are not allowed');
            }
        }

        return this.shardProxies.getBySlot(_slot);
    };
    
    this.getClient4Endpoint = function(endpoint) {
        var proxy = this.shardProxies.get4Endpoint(endpoint);
        if (!proxy) {
            throw new Error('No shard proxy for endpint ' + endpoint);
        }
        return proxy.getClient(false);
    };

    /**
     *
     * @param {Boolean} isRead
     * @param {Array} keys
     * @returns {RedisClient}
     */
    this.getClient = function(isRead, keys) {
        var proxy = this.getProxy(keys);
        if(!proxy) {
            throw new Error('No shard proxy serving for key(s) ' + keys);
        }
        return proxy.getClient(isRead);
    };
    
    /**
     * @param {Boolean} isRead
     * @param {Number} slot
     * @returns {RedisClient} client
     */
    this.getClientOfSlot = function(isRead, slot) {
        return this.shardProxies.getBySlot(slot).getClient(isRead);  
    };

    this.pubsub = new PubSub(function(channel, message) {
        _self.emit('message', channel, message);
        }, function(channel, count) {
            _self.emit('unsubscribe', channel, count);
        }, function(channel, count) {
            _self.emit('punsubscribe', channel, count);
        }, this.LOG
    );
        
    init_commands(this);
}
util.inherits(ClusterClient, EventEmitter);


ClusterClient.prototype.on_ready = function() {
    this.LOG.info('clustedis is ready for requests');
    this.ready = true;
    this.status = 'ready';
    this.emit(this.status);
};

/**
 *
 * @param {Function} done
 */
ClusterClient.prototype.refreshShardProxies = function (shardMap, done) {
    if (this.status == 'refreshing') {
        return;
    }

    this.status = 'refreshing';
    
    var self = this;
    this.LOG.debug(`${this.status} shard proxies`);

    shardMap.forEach(function(shard, key) {
        var proxy = null;
        var existsProxy = self.shardProxies.getBySlot(shard.slotStart);
        if (existsProxy) {
            if (existsProxy.end === shard.slotEnd) {
                proxy = existsProxy;
            } else {
                
                // slot range of shard changed, remove old proxy.
                self.shardProxies.remove(existsProxy);
                
                // proxies whose range is covered by new shard should be voided.
                while(existsProxy && existsProxy.end <= shard.slotEnd) {
                    existsProxy = self.shardProxies.getBySlot(existsProxy.end + 1);
                    self.shardProxies.remove(existsProxy);
                }
            }
        }
        
        if (!proxy) {
            proxy = shardProxy.create(shard, self.redisOptions, self.slaveMode, self.LOG);
            proxy.on('connection_error', function(endpoint) {
                self.stateManager.update();
            });
            self.shardProxies.add(proxy);
        }
    });
    
    done();
};

/**
 * Local shard proxies will be refreshed before invoke callback function if meets redirection.
 * Otherwise, callback directly.
 *
 * @param {Object} err
 * @param {Function} replayFunc
 * @param {Function} next
 */
ClusterClient.prototype.handle_exception = function(err, replayFunc, next) {
    var movedInfo = null;
    var connRefused = false;

    // check error type
    if (err.message.length > 5) {
        movedInfo = err.message.match(/MOVED\s\d+\s([^:]+):(\d+)/i);
        
        if(!movedInfo) {
            // 'Redis connection to 127.0.0.1:7000 failed - connect ECONNREFUSED'
            // 'CLUSTERDOWN The cluster is down'
            connRefused = err.message.indexOf('CLUSTERDOWN') === 0 || err.message.indexOf('ECONNREFUSED') > 0 || err.message.indexOf('connection gone') > 0;
        }
    }

    if (movedInfo && movedInfo[1] && movedInfo[2]) {
        // triger shard info update.
        this.stateManager.update();
        
        // fire redirection event
        this.emit('redirection', err);
        
        var client;
        
        try {
            client = this.getClient4Endpoint({host: movedInfo[1], port: +movedInfo[2]});
        } catch(err) {
            this.LOG.error(err);
        }
        
        if (!client) {
            next();
            return;
        }
        
        replayFunc(client);
        
    } else if(connRefused) {
        
        this.stateManager.update();
        next();
        
    } else {
        next();
    }
};

ClusterClient.prototype.close = function(done) {
    var self = this;
    var numberOfProxies = this.shardProxies.size();
    var numberOfClosed = 0;
  
    this.LOG.debug('clustedis: closing ...');
  
    this.ready = false;
    this.status = 'initializing';
    
    this.stateManager.close();
    
    this.pubsub.close(function() {
        self.shardProxies.map(function(proxy) {
            proxy.close(function() {
                numberOfClosed += 1;
                if(numberOfClosed == numberOfProxies) {
                    self.emit('close');
                    self.LOG.info('clustedis: closed');
                    if(done) {
                        done();
                    }
                }
            });
        });
    });
};

/**
 * Returns the internal redis client of a slot.
 *
 * @param {Boolean} isRead
 * @param {String} key
 * @param {Function} callback
 */
ClusterClient.prototype.getSlotClient = function(isRead, key, callback) {
    if(callback) {
        callback(null, this.getClient(isRead, key));
    }
};

/**
 * Returns true if the command is readonly.
 * 
 * @param {String} command The command name.
 * @returns {Boolean} read only or not
 */
ClusterClient.prototype.isReadOnly = function(command) {
    var cmdInfo = this.commands[command.toLowerCase()];
    if (!cmdInfo) {
        return null;
    }
    return cmdInfo[1][0] == 'readonly';
};

/**
 * Enable transaction.
 * 
 * @returns {Object} transaction context.
 */
ClusterClient.prototype.multi = function(callback) {
    var tran = new Transaction(this);
    if (!!callback) {
        callback(tran);
        tran.exec();
    } else {
        return tran;
    }
};

/**
 * Enable batch (Pipelining).
 * 
 * @returns {Object} pipline context.
 */
ClusterClient.prototype.pipelined = function(callback) {
    var pipeline = new Pipelining(this);
    if(!!callback) {
        callback(pipeline);
        pipeline.exec();
    } else {
        return pipeline;
    }
};


function init_commands(self) {
    for(var cmd in commands) {
        if (cmd == 'subscribe' || cmd == 'unsubscribe' || cmd == 'publish') {
            continue;
        }
        
        if (cmd == 'multi' || cmd == 'exec') {
            continue;
        }
        
        bindCommand(self, cmd);
    }

    ClusterClient.prototype.subscribe = function(channel, done) {
        self.pubsub.subscribe(channel, done);
    };

    ClusterClient.prototype.unsubscribe = function(channel, done) {
        self.pubsub.unsubscribe(channel, done);
    };

    ClusterClient.prototype.publish = function(channel, message, done) {
        self.pubsub.publish(channel, message, done);
    };
}

/**
 * 
 * 
 * @parms {Object} self
 * @param {String} cmd
 */
function bindCommand(self, cmd) {
    ClusterClient.prototype[cmd] = function () {
        
        var isReadCMD = this.isReadOnly(cmd);
        var argsAndCallback = extractArgsAndCallback(arguments);
        var args = argsAndCallback[0];
        
        var next = argsAndCallback[1];
        if (!next) {
            next = emptyCallback;
        }
        
        var keyTable = this.findKeyTable(cmd, args);
        var keys = (keyTable === null) ? null : Array.from(keyTable.values());
        
        var shardProxy;
        
        try {
            shardProxy = self.getProxy(keys);
        } catch (err) {
            next(err);
            return;
        }
        
        if(!shardProxy) {
            next(new Error(`no shard serving for key(s) ' ${keys}`));
            return;
        }
        
        // get the actual redis client from proxy
        var client = shardProxy.getClient(isReadCMD);
        
        if(!client || !client.connected) {
            next(new Error(`shard proxy of ['${shardProxy.start},${shardProxy.end}'] do not have any available connection`));
            return;
        }
        
        // sent command to redis
        client.send_command(cmd, args, function (err, res) {
            if (!!err) {
                // try to deal with exception.
                self.handle_exception(
                    err,
                    function(newClient) { // closure function for replaying current command.
                        newClient.send_command(cmd, args, next);
                    },
                    function() {
                        if (next) {
                            next(err, res);
                        }
                    }
                );
            } else if (next) {
                next(null, res);
            }
        });
    };

    ClusterClient.prototype[cmd.toUpperCase()] = ClusterClient.prototype[cmd];
}

/**
 * Extract command args and callback function from arguments object.
 *
 * @param {Object} args The arguments object passed to function.
 * @returns {Array} [commandArgs, callbackFunction]
 */
function extractArgsAndCallback(args) {

    if(!Array.isArray(args)) {
        var cmdArgs = [];
        var callbackFn = null;
        for(var index in args) {
            if(!isFunction(args[index])) {
                if(Array.isArray(args[index])) {
                    Array.prototype.push.apply(cmdArgs, args[index]);
                } else {
                    cmdArgs.push(args[index]);
                }
            } else {
                callbackFn = args[index];
                break;
            }
        }
        return [cmdArgs, callbackFn];
    } else {

        if (Array.isArray(args[0])) {
            return [args[0], args[1]];
        }

        if (!isFunction(args[args.length - 1])) {
            return [args, null];
        } else {
            return [args, args.pop()];
        }
    }
}

/**
 * Returns true if the input obj is a function.
 *
 * @param {Object} obj
 * @returns {boolean}
 */
function isFunction(obj) {
    return !!(obj && obj.constructor && obj.call && obj.apply);
}

function emptyCallback() {
    // do nothing.
}



/**
 * Finds keys in command args.
 *
 * @param {String} command name
 * @param {Array} args Command args
 * @returns {Map} A index-key pair map.
 */
ClusterClient.prototype.findKeyTable = function(cmd, args) {
    var commandMeta = commands[cmd];
    var keyPosition = commandMeta[2];

    // command do not need a key
    if(!keyPosition) {
        return null;
    }

    var table = new Map();
    
    // store key position and its value.
    // command name is not included in args, so -1.
    table.set(keyPosition - 1, args[keyPosition - 1]);

    var lastKeyPosition = commandMeta[3];
    var keyStep = commandMeta[4];

    // only have one key
    if(keyPosition === lastKeyPosition || !keyStep) {
        return table;
    }

    // command name is not included in args, so -1.
    keyPosition += keyStep - 1;
    while(args[keyPosition]) {
        table.set(keyPosition, args[keyPosition]);
        keyPosition += keyStep;
    }

    return table;
};

module.exports = ClusterClient;