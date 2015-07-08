"use strict";

module.exports = function(clientConstructor) {
    module.router = require('./test/utils/test_router.js')(clientConstructor);
    /*var normalizedPath = require('path').join(__dirname, 'test', 'functional');
    return require("fs").readdirSync(normalizedPath)
    .map(function(file) {
        require('./test/functional/' + file)
    })*/
    return [require('./test/functional/simple.js')];
};


