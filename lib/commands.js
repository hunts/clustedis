'use strict';

/*
 * http://redis.io/commands/command
 * (Redis 3.0.0) `redis-cli command`

 * Each top-level result contains six nested results. Each nested result is:
 *  command name
 *  command arity specification
 *  nested Array reply of command flags
 *  position of first key in argument list
 *  position of last key in argument list
 *  step count for locating repeating keys
 */
var commands = {
    append: [3, ['write', 'denyoom'], 1, 1, 1],
    asking: [1, ['readonly'], 0, 0, 0],
    auth: [2, ['readonly', 'noscript', 'loading', 'stale', 'fast'], 0, 0, 0],
    bgrewriteaof: [1, ['readonly', 'admin'], 0, 0, 0],
    bgsave: [1, ['readonly', 'admin'], 0, 0, 0],
    bitcount: [-2, ['readonly'], 1, 1, 1],
    bitop: [-4, ['write', 'denyoom'], 2, -1, 1],
    bitpos: [-3, ['readonly'], 1, 1, 1],
    blpop: [-3, ['write', 'noscript'], 1, -2, 1],
    brpop: [-3, ['write', 'noscript'], 1, 1, 1],
    brpoplpush: [4, ['write', 'denyoom', 'noscript'], 1, 2, 1],
    client: [-2, ['readonly', 'noscript'], 0, 0, 0],
    cluster: [-2, ['readonly', 'admin'], 0, 0, 0],
    command: [0, ['readonly', 'loading', 'stale'], 0, 0, 0],
    config: [-2, ['readonly', 'admin', 'stale'], 0, 0, 0],
    dbsize: [1, ['readonly', 'fast'], 0, 0, 0],
    debug: [-2, ['admin', 'noscript'], 0, 0, 0],
    decr: [2, ['write', 'denyoom', 'fast'], 1, 1, 1],
    decrby: [3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    del: [-2, ['write'], 1, -1, 1],
    discard: [1, ['readonly', 'noscript', 'fast'], 0, 0, 0],
    dump: [2, ['readonly'], 1, 1, 1],
    echo: [2, ['readonly', 'fast'], 0, 0, 0],
    eval: [-3, ['noscript', 'movablekeys'], 0, 0, 0],
    evalsha: [-3, ['noscript', 'movablekeys'], 0, 0, 0],
    exec: [1, ['noscript', 'skip_monitor'], 0, 0, 0],
    exists: [2, ['readonly', 'fast'], 1, 1, 1],
    expire: [3, ['write', 'fast'], 1, 1, 1],
    expireat: [3, ['write', 'fast'], 1, 1, 1],
    flushall: [1, ['write'], 0, 0, 0],
    flushdb: [1, ['write'], 0, 0, 0],
    get: [2, ['readonly', 'fast'], 1, 1, 1],
    getbit: [3, ['readonly', 'fast'], 1, 1, 1],
    getrange: [4, ['readonly'], 1, 1, 1],
    getset: [3, ['write', 'denyoom'], 1, 1, 1],
    hdel: [-3, ['write', 'fast'], 1, 1, 1],
    hexists: [3, ['readonly', 'fast'], 1, 1, 1],
    hget: [3, ['readonly', 'fast'], 1, 1, 1],
    hgetall: [2, ['readonly'], 1, 1, 1],
    hincrby: [4, ['write', 'denyoom', 'fast'], 1, 1, 1],
    hincrbyfloat: [4, ['write', 'denyoom', 'fast'], 1, 1, 1],
    hkeys: [2, ['readonly', 'sort_for_script'], 1, 1, 1],
    hlen: [2, ['readonly', 'fast'], 1, 1, 1],
    hmget: [-3, ['readonly'], 1, 1, 1],
    hmset: [-4, ['write', 'denyoom'], 1, 1, 1],
    hscan: [-3, ['readonly', 'random'], 1, 1, 1],
    hset: [4, ['write', 'denyoom', 'fast'], 1, 1, 1],
    hsetnx: [4, ['write', 'denyoom', 'fast'], 1, 1, 1],
    hvals: [2, ['readonly', 'sort_for_script'], 1, 1, 1],
    incr: [2, ['write', 'denyoom', 'fast'], 1, 1, 1],
    incrby: [3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    incrbyfloat: [3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    info: [-1, ['readonly', 'loading', 'stale'], 0, 0, 0],
    keys: [2, ['readonly', 'sort_for_script'], 0, 0, 0],
    lastsave: [1, ['readonly', 'random', 'fast'], 0, 0, 0],
    latency: [-2, ['readonly', 'admin', 'noscript', 'loading', 'stale'], 0, 0, 0],
    lindex: [3, ['readonly'], 1, 1, 1],
    linsert: [5, ['write', 'denyoom'], 1, 1, 1],
    llen: [2, ['readonly', 'fast'], 1, 1, 1],
    lpop: [2, ['write', 'fast'], 1, 1, 1],
    lpush: [-3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    lpushx: [3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    lrange: [4, ['readonly'], 1, 1, 1],
    lrem: [4, ['write'], 1, 1, 1],
    lset: [4, ['write', 'denyoom'], 1, 1, 1],
    ltrim: [4, ['write'], 1, 1, 1],
    mget: [-2, ['readonly'], 1, -1, 1],
    migrate: [-6, ['write'], 0, 0, 0],
    monitor: [1, ['readonly', 'admin', 'noscript'], 0, 0, 0],
    move: [3, ['write', 'fast'], 1, 1, 1],
    mset: [-3, ['write', 'denyoom'], 1, -1, 2],
    msetnx: [-3, ['write', 'denyoom'], 1, -1, 2],
    multi: [1, ['readonly', 'noscript', 'fast'], 0, 0, 0],
    object: [3, ['readonly'], 2, 2, 2],
    persist: [2, ['write', 'fast'], 1, 1, 1],
    pexpire: [3, ['write', 'fast'], 1, 1, 1],
    pexpireat: [3, ['write', 'fast'], 1, 1, 1],
    pfadd: [-2, ['write', 'denyoom', 'fast'], 1, 1, 1],
    pfcount: [-2, ['readonly'], 1, 1, 1],
    pfdebug: [-3, ['write'], 0, 0, 0],
    pfmerge: [-2, ['write', 'denyoom'], 1, -1, 1],
    pfselftest: [1, ['readonly'], 0, 0, 0],
    ping: [-1, ['readonly', 'stale', 'fast'], 0, 0, 0],
    psetex: [4, ['write', 'denyoom'], 1, 1, 1],
    psubscribe: [-2, ['readonly', 'pubsub', 'noscript', 'loading', 'stale'], 0, 0, 0],
    psync: [3, ['readonly', 'admin', 'noscript'], 0, 0, 0],
    pttl: [2, ['readonly', 'fast'], 1, 1, 1],
    publish: [3, ['readonly', 'pubsub', 'loading', 'stale', 'fast'], 0, 0, 0],
    pubsub: [-2, ['readonly', 'pubsub', 'random', 'loading', 'stale'], 0, 0, 0],
    punsubscribe: [-1, ['readonly', 'pubsub', 'noscript', 'loading', 'stale'], 0, 0, 0],
    randomkey: [1, ['readonly', 'random'], 0, 0, 0],
    readonly: [1, ['readonly', 'fast'], 0, 0, 0],
    readwrite: [1, ['readonly', 'fast'], 0, 0, 0],
    rename: [3, ['write'], 1, 2, 1],
    renamenx: [3, ['write', 'fast'], 1, 2, 1],
    replconf: [-1, ['readonly', 'admin', 'noscript', 'loading', 'stale'], 0, 0, 0],
    restore: [-4, ['write', 'denyoom'], 1, 1, 1],
    'restore-asking': [-4, ['write', 'denyoom', 'asking'], 1, 1, 1],
    role: [1, ['noscript', 'loading', 'stale'], 0, 0, 0],
    rpop: [2, ['write', 'fast'], 1, 1, 1],
    rpoplpush: [3, ['write', 'denyoom'], 1, 2, 1],
    rpush: [-3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    rpushx: [3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    sadd: [-3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    save: [1, ['readonly', 'admin', 'noscript'], 0, 0, 0],
    scan: [-2, ['readonly', 'random'], 0, 0, 0],
    scard: [2, ['readonly', 'fast'], 1, 1, 1],
    script: [-2, ['readonly', 'noscript'], 0, 0, 0],
    sdiff: [-2, ['readonly', 'sort_for_script'], 1, -1, 1],
    sdiffstore: [-3, ['write', 'denyoom'], 1, -1, 1],
    select: [2, ['readonly', 'loading', 'fast'], 0, 0, 0],
    set: [-3, ['write', 'denyoom'], 1, 1, 1],
    setbit: [4, ['write', 'denyoom'], 1, 1, 1],
    setex: [4, ['write', 'denyoom'], 1, 1, 1],
    setnx: [3, ['write', 'denyoom', 'fast'], 1, 1, 1],
    setrange: [4, ['write', 'denyoom'], 1, 1, 1],
    shutdown: [-1, ['readonly', 'admin', 'loading', 'stale'], 0, 0, 0],
    sinter: [-2, ['readonly', 'sort_for_script'], 1, -1, 1],
    sinterstore: [-3, ['write', 'denyoom'], 1, -1, 1],
    sismember: [3, ['readonly', 'fast'], 1, 1, 1],
    slaveof: [3, ['admin', 'noscript', 'stale'], 0, 0, 0],
    slowlog: [-2, ['readonly'], 0, 0, 0],
    smembers: [2, ['readonly', 'sort_for_script'], 1, 1, 1],
    smove: [4, ['write', 'fast'], 1, 2, 1],
    sort: [-2, ['write', 'denyoom', 'movablekeys'], 1, 1, 1],
    spop: [2, ['write', 'noscript', 'random', 'fast'], 1, 1, 1],
    srandmember: [-2, ['readonly', 'random'], 1, 1, 1],
    srem: [-3, ['write', 'fast'], 1, 1, 1],
    sscan: [-3, ['readonly', 'random'], 1, 1, 1],
    strlen: [2, ['readonly', 'fast'], 1, 1, 1],
    subscribe: [-2, ['readonly', 'pubsub', 'noscript', 'loading', 'stale'], 0, 0, 0],
    substr: [4, ['readonly'], 1, 1, 1],
    sunion: [-2, ['readonly', 'sort_for_script'], 1, -1, 1],
    sunionstore: [-3, ['write', 'denyoom'], 1, -1, 1],
    sync: [1, ['readonly', 'admin', 'noscript'], 0, 0, 0],
    time: [1, ['readonly', 'random', 'fast'], 0, 0, 0],
    ttl: [2, ['readonly', 'fast'], 1, 1, 1],
    type: [2, ['readonly', 'fast'], 1, 1, 1],
    unsubscribe: [-1, ['readonly', 'pubsub', 'noscript', 'loading', 'stale'], 0, 0, 0],
    unwatch: [1, ['readonly', 'noscript', 'fast'], 0, 0, 0],
    wait: [3, ['readonly', 'noscript'], 0, 0, 0],
    watch: [-2, ['readonly', 'noscript', 'fast'], 1, -1, 1],
    zadd: [-4, ['write', 'denyoom', 'fast'], 1, 1, 1],
    zcard: [2, ['readonly', 'fast'], 1, 1, 1],
    zcount: [4, ['readonly', 'fast'], 1, 1, 1],
    zincrby: [4, ['write', 'denyoom', 'fast'], 1, 1, 1],
    zinterstore: [-4, ['write', 'denyoom', 'movablekeys'], 0, 0, 0],
    zlexcount: [4, ['readonly', 'fast'], 1, 1, 1],
    zrange: [-4, ['readonly'], 1, 1, 1],
    zrangebylex: [-4, ['readonly'], 1, 1, 1],
    zrangebyscore: [-4, ['readonly'], 1, 1, 1],
    zrank: [3, ['readonly', 'fast'], 1, 1, 1],
    zrem: [-3, ['write', 'fast'], 1, 1, 1],
    zremrangebylex: [4, ['write'], 1, 1, 1],
    zremrangebyrank: [4, ['write'], 1, 1, 1],
    zremrangebyscore: [4, ['write'], 1, 1, 1],
    zrevrange: [-4, ['readonly'], 1, 1, 1],
    zrevrangebylex: [-4, ['readonly'], 1, 1, 1],
    zrevrangebyscore: [-4, ['readonly'], 1, 1, 1],
    zrevrank: [3, ['readonly', 'fast'], 1, 1, 1],
    zscan: [-3, ['readonly', 'random'], 1, 1, 1],
    zscore: [3, ['readonly', 'fast'], 1, 1, 1],
    zunionstore: [-4, ['write', 'denyoom', 'movablekeys'], 0, 0, 0]
};

/**
 *
 * @param {Object} commandMeta
 * @param {Array} args
 * @returns {Array|String}
 */
function findKeysFromArgs(commandMeta, args) {

    var keyPosition = commandMeta[2];

    // command do not need a key
    if(!keyPosition) {
        return null;
    }

    // init key array with first key.
    var keys = [args[keyPosition]];

    var lastKeyPosition = commandMeta[3];
    var keyStep = commandMeta[4];

    // only have one key
    if(keyPosition === lastKeyPosition || !keyStep) {
        return keys;
    }

    while(args[keyPosition + keyStep]) {
        keys.push(args[keyPosition + keyStep]);
    }

    return keys;
}

exports.commands = commands;
exports.findKeysFromArgs = findKeysFromArgs;