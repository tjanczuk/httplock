#!/usr/bin/env node

var options = {
    port: process.env.PORT || 3001,
    ttl: +process.env.HTTPLOCK_TTL || 15000,
    logger: require('bunyan').createLogger({ name: 'httplock' })
};

require('../lib/httplock').create_server(options, function (error) {
    if (error) {
        options.logger.error(error, 'httplock server failed to establish listener');
        throw error;
    }
    options.logger.warn({ port: options.port, ttl: options.ttl }, 'httplock server started');
});
