[![build status](https://api.travis-ci.org/hunts/clustedis.png)](https://travis-ci.org/hunts/clustedis)
clustedis - a node.js redis cluster client
============================================
# About
  This is a Redis cluster client for the official cluster support (targeted for Redis 3.0) based on node_redis and node-pool.

## Installation

    $ npm install clustedis (not available for now)

## Example

### Step 1 - Create client using factory method

```js
var redis = require('clustedis');

// Should at lease provider one redis server address.
// Other servers will be found via this server.
var client = redis.createClient('127.0.0.1', 30001, {
    debug_mode: false
});
```

### Step 2 - Use client in your code to ready/write data

```js
client.get('a key', function(err, result) {
    // to deal with err or result ;
});
```

## Run Tests

    $ npm install -g gulp
    $ gulp test

## LICENSE - "MIT License"

Copyright (c) 2015 Hunts CHen, http://idf.tf/

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.