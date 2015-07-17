"use strict";

var path = require('path');
var fs = require("fs");
var yaml = require('js-yaml');
var makeRouter = require('./test/utils/test_router.js');

module.exports = {
    test: function(clientConstructor) {
        module.router = makeRouter(clientConstructor);
        var normalizedPath = path.join(__dirname, 'test', 'functional');
        fs.readdirSync(normalizedPath)
        .map(function(file) {
            require('./test/functional/' + file);
        });
    },
    testUtils: require('./test/utils/test_utils.js'),
    spec: yaml.safeLoad(fs.readFileSync(__dirname + '/table.yaml')),
    makeRouter: makeRouter
};


