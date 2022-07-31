"use strict";
var ElasticModule_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.ElasticModule = void 0;
const tslib_1 = require("tslib");
const engine_1 = require("@appolo/engine");
const index_1 = require("../index");
const defaults_1 = require("./src/defaults");
let ElasticModule = ElasticModule_1 = class ElasticModule extends engine_1.Module {
    constructor() {
        super(...arguments);
        this.Defaults = defaults_1.Defaults;
    }
    static for(options) {
        return { type: ElasticModule_1, options };
    }
    get exports() {
        return [{ id: this.moduleOptions.id, type: index_1.ElasticProvider }];
    }
};
ElasticModule = ElasticModule_1 = tslib_1.__decorate([
    (0, engine_1.module)()
], ElasticModule);
exports.ElasticModule = ElasticModule;
//# sourceMappingURL=elasticModule.js.map