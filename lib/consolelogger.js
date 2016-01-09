'use strict';

var debug_mode = /\bclustedis\b/i.test(process.env.NODE_DEBUG);

module.exports = {
    
    DEBUG: debug_mode,
    
    debug: function() {
        if (debug_mode) {
            console.log.apply(null, arguments);
        }
    },
    info: function() {
        console.log.apply(null, arguments);
    },
    warn: function() {
        console.log.apply(null, arguments);
    },
    error: function() {
        console.log.apply(null, arguments);
    }
};