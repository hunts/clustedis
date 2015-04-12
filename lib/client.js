"use strict";
/* jshint loopfunc:true */

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var redis = require('redis');
var commands = require('./commands');
var Pool = require('./pool');
var slotProxy = require('./slot_proxy');
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
    var self = this;

    this.keySlotMap = {}; // Useful for tests. Maintain key-slot mapping manually.
    this.ready = false;
    this.closing = false;
    this.redisNodes = netAddressArray;
    this.redisOptions = redisOptions || {};
    this.poolOptions = poolOptions || {};
    this.slotProxies = new slotProxy.ProxyList();

    this.log = function() {
        if (redisOptions && redisOptions.debug_mode) {
            console.log.apply(null, arguments);
        }
    };

    /**
     *
     * @param {Array} keys
     * @returns {RedisPool}
     */
    this.getPool = function(keys) {

        // do not have key for command
        if(!keys || (Array.isArray(keys) && keys.length === 0)) {
            return this.slotProxies.getFirst().getPool();
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

        var proxy = this.slotProxies.getBySlot(_slot);
        if(proxy === null) {
            throw new Error('No slot serving for key(s) ' + keys);
        }

        return proxy.getPool();
    };

    this.init_slot_proxy(function() {
        self.on_ready();
    });

    this.pubsub = new PubSub();
    init_commands(this);
}
util.inherits(ClusterClient, EventEmitter);


ClusterClient.prototype.on_ready = function() {
    this.log('cluster client is ready for requests');
    this.ready = true;
    this.emit('ready');
};

/**
 *
 * @param {Function} done (SlotProxy Array)
 */
ClusterClient.prototype.init_slot_proxy = function (done) {
    if(!this.redisNodes || this.redisNodes.length === 0) {
        throw new Error('No redis server provided');
    }

    var self = this;
    var createPool = function(host, port) {
        var redisOptions = util._extend({}, self.redisOptions) || {};
        redisOptions.host = host;
        redisOptions.port = port;
        return new Pool(redisOptions, self.poolOptions);
    };

    findClusterDefinition(this.redisNodes, function(err, entryHost, definitions) {

        if(err) {
            throw err;
        }

        var numberOfSlots = definitions.length;
        var numberOfReady = 0;
        self.log('%d slots found', numberOfSlots);
        for(var i = 0; i < numberOfSlots; i++) {
            var slotDef = definitions[i];
            var slotStart = slotDef[0];
            var slotEnd = slotDef[1];
            var proxy = null;
            var existsProxy = self.slotProxies.getBySlot(slotStart);
            if(existsProxy) {
                if(existsProxy.end === slotEnd) {
                    proxy = existsProxy;
                } else {
                    // proxy range changed, remove the old one.
                    self.slotProxies.remove(existsProxy);

                    // proxies whose range is covered by new rang should be voided.
                    while(existsProxy && existsProxy.end <= slotEnd) {
                        existsProxy = self.slotProxies.getBySlot(existsProxy.end + 1);
                        self.slotProxies.remove(existsProxy);
                    }

                    // proxy for new range
                    proxy = slotProxy.create(
                        slotStart, // Start slot range
                        slotEnd, // End slot range
                        createPool
                    );
                    self.slotProxies.add(proxy);
                }
            } else {
                proxy = slotProxy.create(
                    slotStart, // Start slot range
                    slotEnd, // End slot range
                    createPool
                );
                self.slotProxies.add(proxy);
            }

            proxy.on('ready', function() {
                numberOfReady += 1;
                if(numberOfReady == numberOfSlots) {
                    self.pubsub.setPool(this.getPool('PUBSUB'));
                    done();
                }
            });

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
    });
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
ClusterClient.prototype.handle_exception = function(err, done) {

    var redirectInfo = null;

    if (err && err.message && err.message.length > 5) {
        redirectInfo = err.message.match(/MOVED\s+\d+\s+([^:]+):(\d+)/);
    }

    if (redirectInfo && redirectInfo[1] && redirectInfo[2]) {
        this.emit('redirection', err);
        this.init_slot_proxy(function () {
            done(true);
        });
    } else {
        done(false);
    }
};

ClusterClient.prototype.close = function(done) {
    var self = this;
    var numberOfProxies = this.slotProxies.size();
    var numberOfClosed = 0;
    this.ready = false;
    this.closing = true;

    this.pubsub.close(function() {
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

function init_commands(self) {
    for(var command in commands) {
        init_cmd(self, command);
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



function init_cmd(self, cmd) {
    ClusterClient.prototype[cmd] = function () {
        var argsAndFn = extractArgsAndCallback(arguments);
        var keys = findKeysFromArgs(commands[cmd], argsAndFn[0]);
        try {
            var _pool = self.getPool(keys);
            _pool.acquire(function (err, client) {
                client.send_command(cmd, argsAndFn[0], function (err, res) {
                    _pool.release(client);
                    self.handle_exception(err, function(retry) {
                        if(retry) { // Re-execute query.
                            self.log('met exception %s, retry...', err);
                            self[cmd](argsAndFn[0], argsAndFn[1]);
                        } else if (argsAndFn[1]) {
                            argsAndFn[1](err, res);
                        }
                    });
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
function findKeysFromArgs(commandMeta, args) {
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