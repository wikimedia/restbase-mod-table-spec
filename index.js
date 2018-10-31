"use strict";

const path = require('path');
const fs = require("fs");
const yaml = require('js-yaml');

module.exports = {
    test: (clientConstructor) => {
        const makeRouter = require('./test/utils/test_router.js');
        module.router = makeRouter(clientConstructor);
        const normalizedPath = path.join(__dirname, 'test', 'functional');
        fs.readdirSync(normalizedPath)
        .map(file => require(`./test/functional/${file}`));
    },
    getTestUtils: () => require('./test/utils/test_utils.js'),
    spec: yaml.safeLoad(fs.readFileSync(`${__dirname}/table.yaml`)),
    validator: require('./lib/validator')
};


