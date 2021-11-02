const winston = require('winston');
const semver = require('semver');

if (semver.major(winston.version) !== 3) {
    throw new Error(
        "This module only supports winston v3"
    );
}