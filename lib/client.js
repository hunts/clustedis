"use strict";
/* jshint loopfunc:true */

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');
var commands = require('./commands');
var Pool = require('./pool');
var slotProxy = require('./slot_proxy');
var Transaction = require('./transaction');
var Pipelining = require('./pipelining');
var PubSub = require('./pubsub');

/**
 *
 *
 * @param {Array} netAddressArray [{host: string, port: number}]
 * @param {Object} redisOptions
 * @param {Object} poolOptions
 * @constructor
 */
function ClusterClient(netAddressArray, redisOptions, poolOptions) {
    var _self = this;
    var _queuedCallbacks = [];
    this.keySlotMap = {}; // Useful for tests. Maintain key-slot mapping manually.
    this.ready = false;
    this.status = 'initializing';
    this.closing = false;
    this.redisNodes = netAddressArray;
    this.redisOptions = redisOptions || {};
    this.poolOptions = poolOptions || {};
    this.slotProxies = new slotProxy.ProxyList();
    this.queuedCallbacks = [];
    this.numberOfSlots = 0;
    this.commands = commands;

    this.log = function() {
        if (redisOptions && redisOptions.debug_mode) {
            console.log.apply(null, arguments);
        }
    };
    
    /**
     * Push a callback function into queue.
     */
    this.enqueue = function(callback) {
        _queuedCallbacks.push(callback);
    };
    
    /**
     * Pop and returns a callback function from queue.
     * @returns {Function} a callback function
     */
    this.dequeue = function() {
        return _queuedCallbacks.pop();
    };
    
    /**
     * Returns a proxy instance for input key(s). 
     * If do not given any key, return the first proxy in list.
     * If keys belongs to different slot, will raise an error.
     * 
     * @param {Array} keys
     * @returns {SlotProxy}
     */
    this.getProxy = function(keys) {
        // do not have key for command
        if(!keys || (Array.isArray(keys) && keys.length === 0)) {
            return this.slotProxies.getFirst();
        }
        
        if(!Array.isArray(keys)) {
            keys = [keys];
        }

        var _slot = null;

        for(var k=0; k<keys.length; k++) {

            var tmpSlot = this.keySlotMap.hasOwnProperty(keys[k]) ? this.keySlotMap[keys[k]] : slotProxy.calculateSlot(keys[k]);

            this.log('slot of key "%s" is %d', keys[k], tmpSlot);
            if(_slot === null) {
                _slot = tmpSlot;
            } else if(_slot != tmpSlot) {
                throw new Error('Keys are in different slots');
            }
        }

        return this.slotProxies.getBySlot(_slot);
    };

    /**
     *
     * @param {Array} keys
     * @returns {RedisPool}
     */
    this.getPool = function(keys) {
        var proxy = this.getProxy(keys);
        if(proxy === null) {
            throw new Error('No slot serving for key(s) ' + keys);
        }
        return proxy.getPool();
    };
    
    /**
     * @param {Number} slot
     * @returns {RedisPool} pool
     */
    this.getPoolOfSlot = function(slot) {
        return this.slotProxies.getBySlot(slot).getPool();  
    };

    this.init_slot_proxies(function() {
        _self.on_ready();
    });

    this.pubsub = new PubSub();
    init_commands(this);
}
util.inherits(ClusterClient, EventEmitter);


ClusterClient.prototype.on_ready = function() {
    this.log('cluster client is ready for requests');
    this.ready = true;
    this.status = 'ready';
    this.emit(this.status);
};

/**
 *
 * @param {Function} done
 */
ClusterClient.prototype.init_slot_proxies = function (done) {
    if(!this.redisNodes || this.redisNodes.length === 0) {
        throw new Error('No redis server provided');
    }
    
    if (this.status == 'reinitializing') {
        return;
    } else if (this.ready) {
        this.status = 'reinitializing';
    }
    
    this.log('%s slot proxies', this.status);

    var createPool = (function(redisOptions, poolOptions) {
        return function(host, port) {
            var redisOptions = util._extend({}, redisOptions) || {};
            redisOptions.host = host;
            redisOptions.port = port;
            return new Pool(redisOptions, poolOptions);
        };
    })(this.redisOptions, this.poolOptions);

    findClusterDefinition(this.redisNodes, function(err, entryHost, definitions) {

        if(err) {
            throw err;
        }
    
        this.numberOfSlots = definitions.length;
        this.numberOfReady = 0;
        
        var self = this;
        
        for(var i = 0; i < this.numberOfSlots; i++) {
            var slotDef = definitions[i];
            var slotStart = slotDef[0];
            var slotEnd = slotDef[1];
            var proxy = null;
            var existsProxy = this.slotProxies.getBySlot(slotStart);
            if(existsProxy) {
                if(existsProxy.end === slotEnd) {
                    proxy = existsProxy;
                } else {
                    // proxy range changed, remove the old one.
                    this.slotProxies.remove(existsProxy);

                    // proxies whose range is covered by new rang should be voided.
                    while(existsProxy && existsProxy.end <= slotEnd) {
                        existsProxy = this.slotProxies.getBySlot(existsProxy.end + 1);
                        this.slotProxies.remove(existsProxy);
                    }

                    // proxy for new range
                    proxy = slotProxy.create(
                        slotStart, // Start slot range
                        slotEnd, // End slot range
                        createPool
                    );
                    this.slotProxies.add(proxy);
                }
            } else {
                proxy = slotProxy.create(
                    slotStart, // Start slot range
                    slotEnd, // End slot range
                    createPool
                );
                this.slotProxies.add(proxy);
            }
            
            (function(ctx, callback) {
                proxy.once('ready', function() {
                    ctx.numberOfReady += 1;
                    if(ctx.numberOfReady == ctx.numberOfSlots) {
                        ctx.pubsub.setPool(ctx.getPool('PUBSUB'));
                        delete ctx.numberOfReady;
                        callback();
                    }
                });
            })(self, done);

            for(var nodeIndex = 2; nodeIndex < slotDef.length; nodeIndex++) {
                var node = slotDef[nodeIndex];
                if(node[0] == '127.0.0.1') {
                    node[0] = entryHost;
                }
                if(nodeIndex === 2) {
                    proxy.setMaster(node[0], node[1]);
                } else {
                    proxy.addSlave(node[0], node[1]);
                }
            }
        }
    }.bind(this));
};

/**
 *
 * @param {Array} nodes
 * @param {Function} done {err, definitions}
 */
function findClusterDefinition(nodes, done) {
    var proxy = new EventEmitter();
    var failedNodeErrors = [];
    proxy.on('error', function(err) {
        failedNodeErrors.push(err);
        if(failedNodeErrors.length === nodes.length) {
            var finalErrorMessage = 'No available redis server in the given list. Details: ';
            failedNodeErrors.map(function(err) {
                finalErrorMessage += '\n';
                finalErrorMessage += err.message;
            });
            done(new Error(finalErrorMessage));
        }
    });

    proxy.once('data', function(entryHost, definitions) {
        done(null, entryHost, definitions);
    });

    nodes.map(function(node) {
        try {
            var client = redis.createClient(node.port, node.host);
            client.on('error', function (err) {
                proxy.emit('error', err);
                client.end();
            });

            client.on('ready', function () {
                client.send_command('CLUSTER', ['SLOTS'], function(err, definitions) {
                    if(err) {
                        proxy.emit('error', err);
                    } else {
                        proxy.emit('data', client.connectionOption.host, definitions);
                    }
                    client.end(); // release connection.
                });
            });
        } catch (err) {
            proxy.emit('error', err);
        }
    });
}

/**
 * Local slot proxies will be refreshed before invoke callback function if meets redirection.
 * Otherwise, callback directly.
 *
 * @param err
 * @param {Function} done(true if need retry query, otherwise false)
 */
ClusterClient.prototype.handle_exception = function(err, slotProxy, done) {
    
    var redirectInfo = null;
    var connRefused = false;

    // check error type
    if (err.message.length > 5) {
        redirectInfo = err.message.match(/MOVED\s+\d+\s+([^:]+):(\d+)/);
        
        if(!redirectInfo) {
            // 'Redis connection to 127.0.0.1:7000 failed - connect ECONNREFUSED'
            // 'CLUSTERDOWN The cluster is down'
            connRefused = err.message.indexOf('CLUSTERDOWN') === 0 || err.message.indexOf('connect ECONNREFUSED') > 0 || err.message.indexOf('connection gone') > 0;
        }
    }

    if (redirectInfo && redirectInfo[1] && redirectInfo[2]) {
        // When meet MOVE exception, check slots and queue commands for re-play.
        done(true);
        
        this.emit('redirection', err);
        this.init_slot_proxies(function () {
            var callback = null;
            while(!!(callback = this.dequeue())) {
               callback(); 
            }
        }.bind(this));
    } else if(connRefused) {
        // When meet unavailable connection, trigger proxy reset.
        //console.log('meet unavailable connection, trigger proxy reset.');
        setTimeout(function() {
            var nodes = this.redisNodes;
            slotProxy.reset(function(callback) {
                return findClusterDefinition(nodes, callback);
            });
        }.bind(this), 1500);
        done(false);
    } else {
        // Others. Do not retry, but raise error event.
        done(false);
    }
};

ClusterClient.prototype.close = function(done) {
    var self = this;
    var numberOfProxies = this.slotProxies.size();
    var numberOfClosed = 0;
    this.ready = false;
    this.closing = true;

    this.log('closing pubsub ...');
    this.pubsub.close(function() {
        self.log('closed pubsub');
        self.slotProxies.map(function(proxy) {
            proxy.close(function() {
                numberOfClosed += 1;
                if(numberOfClosed == numberOfProxies) {
                    self.closing = false;
                    self.emit('close');
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
 * @param {String} key
 * @param {Function} callback
 * @returns {Function} release function
 */
ClusterClient.prototype.getSlotClient = function(key, callback) {
    var pool = this.getPool(key);
    
    pool.acquire(callback);
    
    return function(client) {
        pool.release(client);
    };
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
ClusterClient.prototype.multi = function() {
    return new Transaction(this);
};

/**
 * 
 */
ClusterClient.prototype.pipelined = function(callback) {
    var pipeline = new Pipelining(this);
    callback(pipeline);
    pipeline.exec();
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

    ClusterClient.prototype.subscribe = function(channel, onMessage) {
        self.pubsub.subscribe(channel, onMessage);
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
        var argsAndFn = extractArgsAndCallback(arguments);
        var keys = this.findKeysFromArgs(commands[cmd], argsAndFn[0]);
        
        try {
            var slotProxy = self.getProxy(keys);
            if(!slotProxy) {
                argsAndFn[1](new Error('no slot serving for key(s) ' + keys));
                return;
            }
            
            if(slotProxy.status == 'initializing') {
                argsAndFn[1](new Error('slot proxy of [' + slotProxy.start + ',' + slotProxy.end + '] is initializing'));
                return;
            }
            
            var wrapCB = (function(self, command, args, callback, slotProxy) {
                return function(err, res) {
                    if (!!err) {
                        // try to deal with exception.
                        self.handle_exception(err, slotProxy, function(doRetry) {
                            
                            // for the command which can be retried, queue it.
                            if(doRetry) {
                                self.log('meet exception %s, queue command for retrying', err);
                                self.enqueue(function() {
                                    self[cmd](command, callback);
                                });
                            } else if (callback) {
                                callback(err, res);
                            }
                        });
                    } else {
                        callback(null, res);
                    }
                };
            })(self, cmd, argsAndFn[0], argsAndFn[1], slotProxy);
            
            // get the connection pool fro proxy instance
            var _pool = slotProxy.getPool();
            
            // acquire an availabe client from pool
            _pool.acquire(function (err, client) {
                
                if (!!err) {
                    wrapCB(err);
                    if (client) {
                        _pool.release(client);
                    }
                    return;
                }
                
                // sent command to redis
                client.send_command(cmd, argsAndFn[0], function (err, res) {
                    
                    // release client into pool first.
                    _pool.release(client);
                    
                    // 
                    wrapCB(err, res);
                });
            });
        } catch (err) {
            if (argsAndFn[1]) {
                argsAndFn[1](err);
            }
        }
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

/**
 * Finds keys from command args.
 *
 * @param {Object} commandMeta
 * @param {Array} args Command args
 * @returns {Array|String} A list of string.
 */
ClusterClient.prototype.findKeysFromArgs = function(commandMeta, args) {
    var keyPosition = commandMeta[2];

    // command do not need a key
    if(!keyPosition) {
        return null;
    }

    // init key array with first key.
    // command name is not included in args, so -1.
    var keys = [args[keyPosition - 1]];

    var lastKeyPosition = commandMeta[3];
    var keyStep = commandMeta[4];

    // only have one key
    if(keyPosition === lastKeyPosition || !keyStep) {
        return keys;
    }

    // command name is not included in args, so -1.
    keyPosition += keyStep - 1;
    while(args[keyPosition]) {
        keys.push(args[keyPosition]);
        keyPosition += keyStep;
    }

    return keys;
}

module.exports = ClusterClient;