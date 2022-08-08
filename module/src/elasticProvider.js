"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ElasticProvider = void 0;
const tslib_1 = require("tslib");
const inject_1 = require("@appolo/inject");
const elasticsearch_1 = require("@elastic/elasticsearch");
const bodybuilder = require("bodybuilder");
const date_1 = require("@appolo/date");
const utils_1 = require("@appolo/utils");
let ElasticProvider = class ElasticProvider {
    get client() {
        return this._client;
    }
    async initialize() {
        this._client = new elasticsearch_1.Client({
            requestTimeout: 60 * 1000 * 10,
            node: this.moduleOptions.connection
        });
    }
    async runSqlQuery(query) {
        const response = await this.client.sql.query({
            body: { query },
        });
        const keys = response.body.columns.map(c => c.name);
        return response.body.rows.map(r => {
            const res = {};
            keys.forEach((key, i) => res[key] = r[i]);
            return res;
        });
    }
    async getById(opts) {
        let dto = {
            index: opts.index,
            id: opts.id
        };
        if (opts.fields && opts.fields.length) {
            dto.body["_source"] = { "includes": opts.fields };
        }
        let { body } = await this.client.get(dto);
        return body;
    }
    searchByAll(opts) {
        return this.searchByQueryBuilder(bodybuilder(), opts);
    }
    async searchByAllMulti(params) {
        let queries = params.map(item => [{ index: item.index }, this._buildQuery(bodybuilder(), item)]);
        try {
            const response = await this.client.msearch({
                body: utils_1.Arrays.flat(queries)
            });
            return response.body.responses.map(res => ({
                total: res.hits.total.value,
                results: res.hits.hits.map(x => Object.assign({ _id: x._id }, x._source))
            }));
        }
        catch (e) {
            this.logger.error(`failed to to run elastic search`, { params: JSON.stringify(params), e });
            throw e;
        }
    }
    searchByQuery(opts) {
        let queryBuild = bodybuilder().query("match", opts.searchField, opts.query);
        return this.searchByQueryBuilder(queryBuild, opts);
    }
    searchByQueryMultiFields(opts) {
        let dto = { query: opts.query };
        if (opts.searchFields && opts.searchFields.length) {
            dto.fields = opts.searchFields;
        }
        let queryBuild = bodybuilder().query("multi_match", dto);
        return this.searchByQueryBuilder(queryBuild, opts);
    }
    searchByTerm(opts) {
        let queryBuild = bodybuilder().query("term", opts.searchField, opts.term);
        return this.searchByQueryBuilder(queryBuild, opts);
    }
    searchByTerms(opts) {
        let queryBuild = bodybuilder().query("terms", opts.searchField, opts.terms);
        return this.searchByQueryBuilder(queryBuild, opts);
    }
    searchByExists(opts) {
        let queryBuild = bodybuilder().query("exists", { field: opts.searchField });
        return this.searchByQueryBuilder(queryBuild, opts);
    }
    searchByQueryBuilder(queryBuilder, opts) {
        let query = this._buildQuery(queryBuilder, opts);
        return this.search(opts.index, query);
    }
    buildQuery(queryBuilder, opts) {
        return this._buildQuery(queryBuilder, opts);
    }
    _buildQuery(queryBuilder, opts) {
        let { fields, pageSize, page, sort, filter, range } = opts;
        pageSize && queryBuilder.size(pageSize);
        page && queryBuilder.from((page - 1) * pageSize);
        if (fields && fields.length) {
            queryBuilder.rawOption("_source", { "includes": fields });
        }
        utils_1.Arrays.forEach(opts.sort, (item, key) => {
            queryBuilder.sort(item.field, item.dir);
        });
        utils_1.Arrays.forEach(filter, (item, key) => {
            queryBuilder.andFilter(item.type || "term", item.field, item.value);
        });
        utils_1.Arrays.forEach(range, item => {
            queryBuilder.andFilter("range", item.field, {
                "gte": item.from,
                "lte": item.to,
            });
        });
        return queryBuilder.build();
    }
    async deleteByTime(opts) {
        try {
            let params = {
                index: opts.index,
                conflicts: "proceed",
                wait_for_completion: false,
                body: {
                    "query": {
                        "bool": {
                            "must": {
                                "range": {
                                    [opts.field]: {
                                        "lte": (0, date_1.date)().utc().subtract(opts.seconds, "seconds").format(opts.format)
                                    }
                                }
                            }
                        }
                    }
                }
            };
            let { body } = await this.client.deleteByQuery(params);
            return body;
        }
        catch (e) {
            this.logger.error(`failed to delete by time `, { e, opts });
            throw e;
        }
    }
    async searchByParams(index, params) {
        return this.search(index, params);
    }
    async search(index, params) {
        try {
            const response = await this.client.search({
                index: index,
                body: params
            });
            return {
                results: response.body.hits.hits.map(x => (Object.assign({ _id: x._id }, x._source))),
                total: response.body.hits.total.value
            };
        }
        catch (e) {
            this.logger.error(`failed to to run elastic search`, { params: JSON.stringify(params), e });
            throw e;
        }
    }
    async exists(index, id) {
        let doc = await this.client.exists({
            id: id,
            index: index,
        });
        return doc.body;
    }
    async create(index, id, item) {
        await this.client.create({
            id: id,
            index: index,
            body: utils_1.Objects.omit(item, "_id", "id")
        });
    }
    async delete(index, id) {
        let isExists = await this.exists(index, id);
        if (!isExists) {
            return;
        }
        await this.client.delete({
            id: id,
            index: index,
        });
    }
    async update(index, id, item) {
        let isExists = await this.exists(index, id);
        if (!isExists) {
            await this.create(index, id, item);
            return;
        }
        await this.client.update({
            id: id,
            index: index,
            body: { doc: utils_1.Objects.omit(item, "_id", "id") }
        });
    }
};
tslib_1.__decorate([
    (0, inject_1.inject)()
], ElasticProvider.prototype, "moduleOptions", void 0);
tslib_1.__decorate([
    (0, inject_1.inject)()
], ElasticProvider.prototype, "logger", void 0);
tslib_1.__decorate([
    (0, inject_1.init)()
], ElasticProvider.prototype, "initialize", null);
ElasticProvider = tslib_1.__decorate([
    (0, inject_1.define)(),
    (0, inject_1.singleton)()
], ElasticProvider);
exports.ElasticProvider = ElasticProvider;
//# sourceMappingURL=elasticProvider.js.map