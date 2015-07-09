"use strict";

require('core-js/shim');

/*
*  test router to exercise all tests uning the restbase-cassandra handler
*/

var RouteSwitch = require('routeswitch');

var router = {};
router.request = function(req) {
    var match = this.newRouter.match(req.uri);
    if (match) {
        req.params = match.params;
        var handler = match.methods[req.method.toLowerCase()];
        if (handler) {
            return handler({}, req)
            .then(function(item){
                return item;
            });
        } else {
            throw new Error('No handler for ' + req.method + ' ' + req.uri);
        }
    } else {
        throw new Error('No match for ' + req.method + ' ' + req.uri);
    }
};

function flatHandlerFromModDef (modDef, prefix) {
    var handler = { paths: {} };
    Object.keys(modDef.spec.paths).forEach(function(path) {
        var pathModSpec = modDef.spec.paths[path];
        handler.paths[prefix + path] = {};
        Object.keys(pathModSpec).forEach(function(m) {
            var opId = pathModSpec[m].operationId;
            if (!modDef.operations[opId]) {
                throw new Error('The module does not export the opration ' + opId);
            }
            handler.paths[prefix + path][m] = modDef.operations[opId];
        });
    });
    return handler;
}

router.makeRouter = function(clientConstructor) {
    var self = this;
    return clientConstructor()
    .then(function(modDef) {
        self.newRouter = new RouteSwitch.fromHandlers([flatHandlerFromModDef(modDef, '/{domain}/sys/table')]);
        return self;
    });
};

module.exports = function(clientConstructor) {
    router.setup = function() {
        return this.makeRouter(clientConstructor);
    };
    return router;
};
