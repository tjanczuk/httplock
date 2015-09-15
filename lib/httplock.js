var http = require('http')
    , url = require('url');

exports.create_server = function (options, callback) {

    if (!options || typeof options != 'object')
        throw new Error('options must be specified');
    if (isNaN(options.port))
        throw new Error('options.port must be provided');

    if (isNaN(options.ttl)) options.ttl = 15000;
    if (isNaN(options.max_body)) options.max_body = 10 * 1024;
    
    options.logger = options.logger || require('bunyan').createLogger({ name: 'httplock'});

    var lowr = {};
    var lowr_stats = {
        grant: 0,
        wait: 0,
        renew: 0,
        release: 0,
        expire: 0
    };

    var server = http.createServer(function (req, res) {
        var body;
        var done;

        req.on('data', function (chunk) {
            body = (body || '') + chunk;
            if (body.length > options.max_body) {
                error(413);
            }
        });
        req.on('end', function () {
            if (done) return;
            if (body) {
                try {
                    body = JSON.parse(body);
                }
                catch (e) {
                    error(400, { code: 400, error: 'Unable to parse request body as JSON.'});
                }
            }
            route();
        });

        function route() {
            if (done) return;

            req.url = url.parse(req.url, true);
            var segment = req.url.pathname.substring(1).split('/');
            if (req.method === 'GET' && segment[0] === 'status') {
                options.logger.info('status request');
                var result = { 
                    lowr: {},
                    uptime: process.uptime(),
                    memory: process.memoryUsage(),
                    lowr_stats: lowr_stats
                };
                for (var i in lowr) {
                    result.lowr[i] = {
                        owner: lowr[i].owner,
                        age: Date.now() - lowr[i].created,
                        created: lowr[i].created,
                        renew_count: lowr[i].renew_count,
                        wait_queue: lowr[i].wait_queue.length
                    };
                }
                res.writeHead(200, { 
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache' 
                });
                return res.end(JSON.stringify(result));
            }
            else if (req.method === 'POST' && segment[0] === 'lowr' && segment[1]) {
                // Get or extend a lock
                var entry = lowr[segment[1]];
                if (entry) {
                    if (entry.owner === req.url.query.owner) {
                        // Lock prevously taken by the same owner - extend
                        lowr_stats.renew++;
                        clearTimeout(entry.ttl);
                        entry.ttl = setTimeout(lowr_terminate(segment[1]), options.ttl);
                        entry.ttl.unref();
                        entry.renew_count++;
                        res.writeHead(201, { 'Content-Type': 'application/json' });
                        var data = { 
                            name: segment[1],
                            ttl: options.ttl, 
                            owner: entry.owner,
                            age: Date.now() - entry.created,
                            renew_count: entry.renew_count,
                            wait_queue: entry.wait_queue.length
                        };
                        options.logger.info(data, 'lock renewed');
                        res.end(JSON.stringify(data));
                    }
                    else {
                        // Lock already taken - wait
                        lowr_stats.wait++;
                        entry.wait_queue.push({ res: res });
                        options.logger.info({ 
                            name: segment[1],
                            ttl: options.ttl, 
                            owner: entry.owner,
                            age: Date.now() - entry.created,
                            renew_count: entry.renew_count,
                            wait_queue: entry.wait_queue.length
                        }, 'lock taken, waiting');
                    }
                }
                else {
                    // Lock does not exist - take it
                    lowr_stats.grant++;
                    entry = lowr[segment[1]] = {
                        owner: req.url.query.owner,
                        ttl: setTimeout(lowr_terminate(segment[1]), options.ttl),
                        wait_queue: [],
                        renew_count: 0,
                        created: Date.now()
                    };
                    entry.ttl.unref();
                    options.logger.info({ 
                        name: segment[1],
                        ttl: options.ttl, 
                        owner: entry.owner,
                        age: Date.now() - entry.created,
                        renew_count: entry.renew_count,
                        wait_queue: entry.wait_queue.length
                    }, 'lock obtained');
                    res.writeHead(201, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ 
                        name: segment[1],
                        ttl: options.ttl, 
                        owner: entry.owner,
                        renew_count: 0, 
                        age: 0 
                    }));
                }
            }
            else if (req.method === 'PUT' && segment[0] === 'lowr' && segment[1]) {
                // Release lock
                var entry = lowr[segment[1]];
                if (entry) {
                    if (entry.owner !== req.url.query.owner) {
                        // Not an owner of the lock
                        options.logger.info({ 
                            name: segment[1],
                            ttl: options.ttl, 
                            owner: entry.owner,
                            attempted_owner: req.url.query.owner,
                            age: Date.now() - entry.created,
                            renew_count: entry.renew_count,
                            wait_queue: entry.wait_queue.length
                        }, 'lock obtained');
                        error(409);
                    }
                    else {
                        // Release waiting clients
                        clearTimeout(entry.ttl);
                        entry.ttl = undefined;
                        options.logger.info({ 
                            name: segment[1],
                            ttl: options.ttl, 
                            owner: entry.owner,
                            age: Date.now() - entry.created,
                            renew_count: entry.renew_count,
                            wait_queue: entry.wait_queue.length,
                            has_body: !!body
                        }, 'lock released');
                        lowr_terminate(segment[1], body || {})();
                        res.writeHead(200);
                        res.end();
                    }
                }
                else {
                    error(404);
                }
            }
            else {
                error (404);
            }
        }

        function lowr_terminate(lock_name, msg) {
            return function () {
                var entry = lowr[lock_name];
                if (entry) {
                    if (msg) {
                        lowr_stats.release++;
                    }
                    else {
                        lowr_stats.expire++;
                    }
                    entry.wait_queue.forEach(function (client) {
                        try {
                            if (msg) {
                                // Lock was properly released
                                client.res.writeHead(200, { 'Content-Type': 'application/json' });
                                client.res.end(JSON.stringify(msg));
                            }
                            else {
                                // Lock expired
                                client.res.writeHead(408);
                            }
                        }
                        catch (e) {
                            // ignore   
                        }
                    });
                    delete lowr[lock_name];
                }
            }
        }

        function error(code, msg) {
            done = true;
            options.logger.warn({ 
                name: segment[1],
                method: req.method,
                path: req.url.pathname || req.url,
                code: code,
                msg: msg
            }, 'http error');
            res.writeHead(code);
            msg ? res.end(JSON.stringify(msg)) : res.end();
        }

    }).listen(options.port, callback);

    return server;
};


exports.get_status = function (options, callback) {
    if (!options || typeof options != 'object')
        throw new Error('options must be specified');

    var request = require('request');

    if (isNaN(options.ttl)) options.ttl = 20000;
    options.url = options.url || 'http://localhost:3001';

    request({
        url: options.url + '/status',
        method: 'GET',
        timeout: options.ttl
    }, function (err, res, body) {
        if (err) return callback(err);
        if (res.statusCode === 200) {
            return callback(null, JSON.parse(body));
        }
        else {
            return callback(new Error('Unexpected response: ' + res.statusCode));
        }
    });
};


exports.take_lowr = function (options, callback) {

    if (!options || typeof options != 'object')
        throw new Error('options must be specified');

    var request = require('request');

    if (isNaN(options.ttl)) options.ttl = 20000;
    options.url = options.url || 'http://localhost:3001';
    options.name = options.name || 'singleton';
    options.owner = options.owner || (Date.now() + '.' + process.pid);
    options.logger = options.logger || require('bunyan').createLogger({ name: 'httplock'});

    var renew_timer;
    var renew_timeout;
    var done_called;
    var release_called;
    request({
        url: options.url + '/lowr/' + options.name + '?owner=' + options.owner,
        method: 'POST',
        timeout: options.ttl
    }, function (err, res, body) {
        if (err) return callback(err);
        if (res.statusCode === 201) {
            // Lock taken successfuly, return release function
            options.logger.info({ 
                req_id: options.req_id,
                name: options.name
            }, 'lowr lock obtained');
            body = JSON.parse(body);
            renew_timeout = Math.round((body.ttl || 15000) / 2);
            renew_timer = setTimeout(renew, renew_timeout);
            renew_timer.unref();
            return done(null, { release: release });
        }
        else if (res.statusCode === 200) {
            // Lock returned work result, return it
            options.logger.info({ 
                req_id: options.req_id,
                name: options.name
            }, 'lowr lock returned with result');
            return done(null, { data: JSON.parse(body) });
        }
        else if (res.statusCode === 408) {
            // Lock expired on the server
            options.logger.warn({ 
                req_id: options.req_id,
                name: options.name
            }, 'lowr ttl expired on the server');
            return done(new Error("httplock server released lock after TTL expiry."));
        }
        else {
            options.logger.warn({ 
                req_id: options.req_id,
                name: options.name,
                code: res.statusCode
            }, 'lowr server responded with unsupported status code');
            return done(new Error("httplock responded with unsupported code: " + res.statusCode));
        }
    });
    
    function done(error, result) {
        if (done_called) return;
        done_called = true;
        if (renew_timeout) {
            clearTimeout(renew_timeout);
            renew_timeout = undefined;
        }
        callback(error, result);
    }

    function renew() {
        renew_timer = undefined;
        request({
            url: options.url + '/lowr/' + options.name + '?owner=' + options.owner,
            method: 'POST',
            timeout: renew_timeout
        }, function (err, res, body) {
            if (err) {
                // Lock not extended
                return options.logger.warn({
                    req_id: options.req_id,
                    name: options.name,
                    err: err.message || err
                }, 'lowr renew failed');
            }
            if (res.statusCode !== 201) {
                // Lock not extended
                return options.logger.warn({
                    req_id: options.req_id,
                    name: options.name,
                    status_code: res.statusCode
                }, 'lowr renew failed');
            }
            options.logger.info({
                req_id: options.req_id,
                name: options.name
            }, 'lowr renewed');
            if (!release_called) {
                renew_timer = setTimeout(renew, renew_timeout);
            }
        });
    }

    function release(data, cb) {
        if (release_called) {
            return cb && cb(new Error('Cannot call release more than once.'));
        }
        release_called = true;

        if (renew_timer) {
            clearTimeout(renew_timer);
            renew_timer = undefined;
        }

        var release_opts = {
            url: options.url + '/lowr/' + options.name + '?owner=' + options.owner,
            method: 'PUT',
            timeout: renew_timeout
        };
        if (data) {
            release_opts.headers = {
                'Content-Type': 'application/json'
            };
            release_opts.body = JSON.stringify(data);
        }
        request(release_opts, function (err, res, body) {
            if (err) {
                options.logger.warn({
                    req_id: options.req_id,
                    name: options.name,
                    err: err.message || err
                }, 'lowr release failed');
                return cb && cb(err);
            }
            if (res.statusCode !== 200) {
                options.logger.warn({
                    req_id: options.req_id,
                    name: options.name,
                    status_code: res.statusCode
                }, 'lowr release failed');
                return cb && cb(new Error('httplock responded with error status code: ' + res.statusCode));
            }
            options.logger.info({
                req_id: options.req_id,
                name: options.name
            }, 'lowr released');
            return cb && cb();
        });
    }
}
