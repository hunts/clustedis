"use strict";
/* jshint loopfunc:true */

var commands = require('./commands');
var calculateSlot = require('./shardproxy').calculateSlot;

function Pipelining(clusterClient) {
	
	var slotMap = new Map();
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
		if (!keyTable || keyTable.size === 0) {
			slot = -1; // solt:-1 for commands which do not target to a particular slot.
		} else if (keyTable.size === 1) {
			slot = calculateSlot(keyTable.values().next().value);
		} else {
			throw new Error('pipelining do not support mset/mget and etc with multi keys assigned');
		}
		
		if (!slotMap.has(slot)) {
			slotMap.set(slot, new Map());
		}
		
		slotMap.get(slot).set(sequenceId, [cmd, args]);
		sequenceId += 1;
	};
	
	/**
	 * 
	 * @param {Function} done {err, replies}
	 */
	this.exec = function(done) {
		
		if (slotMap.size === 0) {
			if (!!done) {
				done(null, []);
			}
			return;
		}
		
		var slots = Array.from(slotMap.keys());
		
		if (slots.length === 1) {
			if (slots[0] === -1) {
				// copy data in slot:-1 to slot:0
				slotMap.set(0, slotMap.get(-1));
				slotMap.delete(-1);
				slots[0] = 0;
			}
		}
		
		// merge data in slot:-1 into any other slot.
		if (slots.length > 1 && slotMap.has(-1)) {
			for (var i in slots) {
				if (slots[i] !== -1) {
					slotMap.get(-1).forEach(function(commandData, sequence) {
						slotMap.get(slots[i]).set(sequence, commandData);
					});
					slotMap.delete(-1);
					slots = Array.from(slotMap.keys());
					break;
				}
			}
		}
		
		var finalError = null;
		var finalReplies = [];
		var self = this;
		var numberOfTasks = slots.length;
		
		slots.map(function(slot) {
			var client = self.client.getClientOfSlot(false, slot);
			var sequencedCommandMap = slotMap.get(slot);
			var batch = client.batch();
			sequencedCommandMap.forEach(function(commandData, sequence) {
				batch[commandData[0]](...commandData[1]);
			});
			
			batch.exec(function(err, replies) {
				finalError = err;
				
				if (!err) {
					var i = 0;
					sequencedCommandMap.forEach(function(commandData, sequence) {
						finalReplies[sequence] = replies[i];
						i++;
					});
				}
				
				numberOfTasks -= 1;
				if (numberOfTasks === 0 && !!done) {
					done(finalError, finalReplies);
				}
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