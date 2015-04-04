'use strict';

var events = require('events');
var util = require('util');
var cmd = require('./commands');
var pool = require('./pool');
var slot = require('./slot');
var PubSub = require('./pubsub');

function ClusterClient(netAddressArray, options) {
    var self = this;
    var _slotProxyArray = [];
    var _client = pool.redis.createClient(netAddressArray[0].port, netAddressArray[0].host);
    _client.on('ready', function() {
        _client.send_command('CLUSTER', ['SLOTS'], function(err, definitions) {
            var numberOfSlots = definitions.length;
            var numberOfReady = 0;
            for(var i = 0; i < numberOfSlots; i++) {
                var slotDef = definitions[i];
                var slotInstance = slot.createSlot(
                    slotDef[0], // Start slot range
                    slotDef[1] // End slot range
                );

                slotInstance.on('ready', function() {
                    numberOfReady += 1;
                    if(numberOfReady == numberOfSlots) {
                        self.on_ready();
                    }
                });

                _slotProxyArray.push(slotInstance);

                for(var nodeIndex = 2; nodeIndex < slotDef.length; nodeIndex++) {
                    var node = slotDef[nodeIndex];
                    if(node[0] == '127.0.0.1') {
                         node[0] = _client.connectionOption.host;
                    }
                    if(nodeIndex === 2) {
                        slotInstance.setMaster(node[0], node[1]);
                    } else {
                        slotInstance.addSlave(node[0], node[1]);
                    }
                }

                slotInstance.initPool(pool, options);
            }

            _client.quit();
        });
    });

    /**
     *
     * @param {Array} keys
     * @returns {RedisPool}
     */
    this.getPool = function(keys) {

        // do not have key for command
        if(!keys || keys.length === 0) {
            return _slotProxyArray[0].getPool();
        }

        var _slot = null;

        for(var k=0; k<keys.length; k++) {
            var tmpSlot = slot.calculateSlot(keys[k]);
            console.log('slot of key "%s" is %d', keys[k], tmpSlot);
            if(_slot === null) {
                _slot = tmpSlot;
            } else if(_slot != tmpSlot) {
                throw new Error('Keys are in different slots');
            }
        }

        for(var s=0; s<_slotProxyArray.length; s++) {
            if(_slotProxyArray[s].accept(_slot)) {
                return _slotProxyArray[s].getPool();
            }
        }

        throw new Error('No slot serving for key ' + key);
    };
    this.status = 'init';
    this.pubsub = new PubSub();
    init_commands(this);
    events.EventEmitter.call(this);
}
util.inherits(ClusterClient, events.EventEmitter);


ClusterClient.prototype.on_ready = function() {
    console.log('cluster client is ready');
    this.pubsub.setPool(this.getPool('PUBSUB'));
    this.status = 'ready';
    this.emit('ready');
};

function init_commands(self) {
    var _prototype = ClusterClient.prototype;
    for(var command in cmd.commands) {
        _prototype[command] = function() {
            var argsAndFn = extractArgsAndCallback(arguments);
            var keys = cmd.findKeysFromArgs(cmd.commands[command], argsAndFn[0]);
            var _pool = self.getPool(keys);
            _pool.acquire(function(err, client) {
                client.send_command(command, argsAndFn[0], function(err, res) {
                    _pool.release(client);
                    if(argsAndFn[1]) {
                        argsAndFn[1](err, res);
                    }
                });
            });
        };

        _prototype[command.toUpperCase()] = _prototype[command];
    }

    _prototype['subscribe'] = function(channel, onMessage) {
        self.pubsub.subscribe(channel, onMessage);
    };

    _prototype['publish'] = function(channel, message) {
        self.pubsub.publish(channel, message);
    };
};



/**
 *
 * @param {Array} args
 * @returns {Array}
 */
function extractArgsAndCallback(args) {
    if(Array.isArray(args[0])) {
        return [args[0], args[1]];
    }

    if(!isFunction(args[args.length -1])) {
        return [args, null];
    } else {
        return [args, args.pop()];
    }
};

/**
 *
 * @param {Object} obj
 * @returns {boolean}
 */
function isFunction(obj) {
    return !!(obj && obj.constructor && obj.call && obj.apply);
};

module.exports = ClusterClient;