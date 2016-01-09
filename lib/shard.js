'use strict';

/**
 * @param {Number} slotStart
 * @param {Number} slotEnd
 * @param {Object} master
 * @param {Array} slaves
 */
function Shard(slotStart, slotEnd, master, slaves) {
    var self = this;
    this.slotStart = slotStart;
    this.slotEnd = slotEnd;
    this.master = master;
    this.slaveMap = new Map();
    
    slaves.forEach(function(slave, index) {
        self.slaveMap.set(`${slave.host}:${slave.port}`, slave);
    });
}

module.exports = Shard;