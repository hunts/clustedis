"use strict";
/* jshint loopfunc:true */

var commands = require('./commands');
var calculateSlot = require('./slot_proxy').calculateSlot;

function Pipelining(clusterClient) {
	
	var slotMap = {};
	
	/**
	 * 
	 */
	this.client = clusterClient;
	
	/**
	 * 
	 */
	this.enqueue = function(cmd, args) {
		var keyTable = this.client.findKeyTable(cmd, args);
		var slot = null;
		if (!keyTable || keyTable.count() === 0) {
			slot = -1; // solt:-1 for commands which do not target to a particular slot.
		} else if (keyTable.count() > 1) {
			throw new Error('transaction do not support mset/mget and etc with multi keys assigned');
		} else {
			slot = calculateSlot(keyTable.values()[0]);
		}
		
		if (!slotMap[slot]) {
			slotMap[slot] = [];
		}
		
		slotMap[slot].push([cmd, args]);
	};
	
	/**
	 * 
	 * @param {Function} callback {err, replies}
	 */
	this.exec = function() {
		
		var slots = Object.getOwnPropertyNames(slotMap);
		if (slots.length === 0) {
			return;
		}
		
		if (slots.length == 1) {
			if (slots[0] == '-1') {
				// copy data in slot:-1 to slot:0
				slotMap['0'] = slotMap['-1'];
				delete slotMap['-1'];
				slots[0] = '0';
			}
		}
		
		// merge data in slot:-1 into any other slot.
		if (slots.length > 1 && slotMap.hasOwnProperty('-1')) {
			for (var i in slots) {
				if (slots[i] != '-1') {
					slotMap['-1'].map(function(item) {
						slotMap[slots[i]].push(item);
					});
					delete slotMap['-1'];
					slots = Object.getOwnPropertyNames(slotMap);
					break;
				}
			}
		}
		
		var self = this;
		
		slots.map(function(slot) {
			var pool = self.client.getPoolOfSlot(slot);
			var commandDataArray = slotMap[slot];
			pool.acquire(function(err, client) {
				
				var numberOfCommands = commandDataArray.length;
				
				var ensureReleaseClient = function() {
						numberOfCommands -= 1;
						if (numberOfCommands === 0) {
							pool.release(client);
						}
				};
				
				commandDataArray.map(function(commandData) {
					if (commandData.length === 1) {
						commandData.push([]);
					}
					
					var cmd = commandData[0];
					var allArgs = commandData[1];
					
					if (!allArgs) {
						allArgs = [];
					}
					
					if (allArgs.length === 0) {
						client[cmd](allArgs, ensureReleaseClient);
					} else {
						var lastArg = allArgs[allArgs.length - 1];
						// Check if the last arg is callback function
						if (lastArg && lastArg.constructor && lastArg.call && lastArg.apply) {
							allArgs.pop();
							client[cmd](allArgs, function(err, res) {
								ensureReleaseClient();
								lastArg(err, res);
							});
						} else {
							client[cmd](allArgs, ensureReleaseClient);
						}
					}
				});
			});
		});
	};
}

for (var cmd in commands) {
	
	if (cmd == 'subscribe' || cmd == 'unsubscribe' || cmd == 'publish') {
		continue;
	}
	
	if (cmd == 'multi' || cmd == 'exec') {
		continue;
	}
	
	(function(cmd) {
		Pipelining.prototype[cmd] = function() {
			this.enqueue(cmd, Array.prototype.slice.call(arguments));
			return this;
		};
	})(cmd);
}

module.exports = Pipelining;