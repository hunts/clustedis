"use strict";
/* jshint loopfunc:true */

var commands = require('./commands');
var calculateSlot = require('./slot_proxy').calculateSlot;

function Transaction(clusterClient) {
	var slotMap = {};
	var sequenceId = 0;
	
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
			slotMap[slot] = {};
		}
		
		slotMap[slot][sequenceId] = [cmd, args];
		
		sequenceId += 1;
	};
	
	/**
	 * 
	 * @param {Function} callback {err, replies}
	 */
	this.exec = function(callback) {
		
		var slots = Object.getOwnPropertyNames(slotMap);
		if (slots.length === 0) {
			callback(null, []);
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
					Object.getOwnPropertyNames(slotMap['-1']).map(function(sequence) {
						slotMap[slots[i]][sequence] = slotMap['-1'][sequence];
					});
					delete slotMap['-1'];
					slots = Object.getOwnPropertyNames(slotMap);
					break;
				}
			}
		}
		
		var finalError = null;
		var finalReplies = [];
		var self = this;
		var numberOfTasks = slots.length;
		
		slots.map(function(slot) {
			var pool = self.client.getPoolOfSlot(slot);
			var sequencedCommands = slotMap[slot];
			pool.acquire(function(err, client) {
				var x = client.multi();
				var sequenceArray = Object.getOwnPropertyNames(sequencedCommands);
				sequenceArray.map(function(sequence) {
					var commandData = sequencedCommands[sequence];
					x[commandData[0]].apply(x, commandData[1]);
				});
				
				x.exec(function(err, replies) {
					
					pool.release(client);
					
					finalError = err;
					
					if (!err) {
						var i = 0;
						sequenceArray.map(function(sequence) {
							finalReplies[sequence] = replies[i];
							i++;
						});
					}
					
					numberOfTasks -= 1;
					if (numberOfTasks === 0) {
						callback(finalError, finalReplies);
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
		Transaction.prototype[cmd] = function() {
			this.enqueue(cmd, arguments);
			return this;
		};
	})(cmd);
}

module.exports = Transaction;