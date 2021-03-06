[![NPM version][npm-image]][npm-url]
[![build status][travis-image]][travis-url]
[![Coverage Status][coveralls-image]][coveralls-url]

# About
  This is a Redis cluster client for the official cluster support (targeted for Redis 3.0) based on redis(node_redis).
# Important
  This client is still under development. Neither all planned features have been implemented, nor tests covered.

  It couldn't be used in any production application until 0.1.0 released.

## Installation

```bash
$ npm install clustedis
```

## Example

### Step 1 - Create client using factory method

```js
var redis = require('clustedis');

// Other node(s) in the cluster can be discovered by this node.
var client = redis.createClient('127.0.0.1', 7000);
```

A redis cluster always contains more than one node. The cluster can be initialized by providing a node list.

```js
var redis = require('clustedis');

// Other node(s) in the cluster not provided explicitly can be discovered by these node.
var client = redis.createClient(['127.0.0.1:7000', '127.0.0.1:7001']);
```


### Step 2 - Use client in your code to read/write data

```js
client.get('a key', function(err, result) {
    // to deal with err or result ;
});
```

## Run Tests

  To run the test suite, first install the dependencies, then run `npm test`:

```bash
$ npm install
$ npm test
```

## LICENSE - "MIT License"

Copyright (c) 2015 Hunts Chen, http://idf.tf/

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

[npm-url]: https://npmjs.org/package/clustedis
[npm-image]: https://img.shields.io/npm/v/clustedis.svg
[coveralls-url]: https://coveralls.io/github/hunts/clustedis?branch=master
[coveralls-image]: https://coveralls.io/repos/hunts/clustedis/badge.svg?branch=master&service=github
[travis-url]: https://travis-ci.org/hunts/clustedis
[travis-image]: https://api.travis-ci.org/hunts/clustedis.svg
