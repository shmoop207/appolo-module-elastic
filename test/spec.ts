import chai = require('chai');
import sinonChai = require("sinon-chai");
import {App, createApp} from '@appolo/engine'
import {LoggerModule} from '@appolo/logger'
import {ElasticModule} from "../module/elasticModule";
import {ElasticProvider} from "../module/src/elasticProvider";

let should = require('chai').should();
chai.use(sinonChai);

describe("elastic module Spec", function () {
    let app: App;

    beforeEach(async () => {

        app = createApp({root: process.cwd() + '/test/mock/', environment: "production"});
        app.module.use(LoggerModule);
        app.module.use(ElasticModule.for({
            connection: ""
        }));

        await app.launch();
    })

    it("should get provider", async () => {

        let provider = app.injector.get("elasticProvider")

        provider.should.be.ok;
    })



});
