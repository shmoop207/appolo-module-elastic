"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const chai = require("chai");
const sinonChai = require("sinon-chai");
const engine_1 = require("@appolo/engine");
const logger_1 = require("@appolo/logger");
const elasticModule_1 = require("../module/elasticModule");
let should = require('chai').should();
chai.use(sinonChai);
describe("elastic module Spec", function () {
    let app;
    beforeEach(async () => {
        app = (0, engine_1.createApp)({ root: process.cwd() + '/test/mock/', environment: "production" });
        app.module.use(logger_1.LoggerModule);
        app.module.use(elasticModule_1.ElasticModule.for({
            connection: ""
        }));
        await app.launch();
    });
    it("should get provider", async () => {
        let provider = app.injector.get("elasticProvider");
        provider.should.be.ok;
    });
});
//# sourceMappingURL=spec.js.map