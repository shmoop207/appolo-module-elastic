import {define, inject, singleton, alias, init} from "@appolo/inject";
import {Client, RequestParams} from '@elastic/elasticsearch';
import {IElasticResult, IElasticSearchParams, IOptions} from "./interfaces";
import * as bodybuilder from 'bodybuilder';
import * as _ from 'lodash';
import * as moment from 'moment';
import {ILogger} from '@appolo/logger';

@define()
@singleton()
export class ElasticProvider {

    @inject() private moduleOptions: IOptions;
    @inject() private logger: ILogger;

    private _client: Client;

    public get client() {
        return this._client;
    }

    @init()
    private async initialize() {
        this._client = new Client({
            requestTimeout: 60 * 1000 * 10,
            node: this.moduleOptions.connection
        });
    }

    public async runSqlQuery<T>(query: string): Promise<T[]> {
        const response = await this.client.sql.query({
            body: {query},
        });

        const keys = response.body.columns.map(c => c.name);

        return response.body.rows.map(r => {
            const res = {};
            keys.forEach((key, i) => res[key] = r[i]);
            return res;
        });
    }

    public async getById<T>(opts: { id: string, index: string, fields?: string[] }): Promise<T> {

        let dto: any = {
            index: opts.index,
            id: opts.id
        };

        if (opts.fields && opts.fields.length) {
            dto.body["_source"] = {"includes": opts.fields};
        }

        let {body} = await this.client.get(dto);

        return body as T
    }

    public searchByAll<T>(opts: IElasticSearchParams): Promise<IElasticResult<T>> {

        return this.searchByQueryBuilder(bodybuilder(), opts);
    }

    public async searchByAllMulti<T>(params: IElasticSearchParams[]): Promise<IElasticResult<T>[]> {

        let queries = params.map(item => [{index: item.index}, this._buildQuery(bodybuilder(), item)]);

        try {

            const response = await this.client.msearch({
                body: _.flatten(queries)
            });


            return response.body.responses.map(res => ({
                total: res.hits.total.value,
                results: res.hits.hits.map(x => Object.assign({_id: x._id}, x._source))
            }));

        } catch (e) {
            this.logger.error(`failed to to run elastic search`, {params: JSON.stringify(params), e});
            throw e;
        }

    }

    public searchByQuery<T>(opts: { query: string, searchField: string } & IElasticSearchParams): Promise<IElasticResult<T>> {

        let queryBuild = bodybuilder().query("match", opts.searchField, opts.query);

        return this.searchByQueryBuilder(queryBuild, opts);
    }

    public searchByQueryMultiFields<T>(opts: { query: string, searchFields?: string[] } & IElasticSearchParams): Promise<IElasticResult<T>> {

        let dto: any = {query: opts.query};

        if (opts.searchFields && opts.searchFields.length) {
            dto.fields = opts.fields
        }

        let queryBuild = bodybuilder().query("multi_match", dto);

        return this.searchByQueryBuilder(queryBuild, opts);
    }

    public searchByTerm<T>(opts: { term: string, searchField: string } & IElasticSearchParams): Promise<IElasticResult<T>> {

        let queryBuild = bodybuilder().query("term", opts.searchField, opts.term);

        return this.searchByQueryBuilder(queryBuild, opts);
    }

    public searchByTerms<T>(opts: { terms: string[], searchField: string } & IElasticSearchParams): Promise<IElasticResult<T>> {

        let queryBuild = bodybuilder().query("terms", opts.searchField, opts.terms);

        return this.searchByQueryBuilder(queryBuild, opts);
    }

    public searchByExists<T>(opts: { searchField: string } & IElasticSearchParams): Promise<IElasticResult<T>> {

        let queryBuild = bodybuilder().query("exists", {field: opts.searchField});

        return this.searchByQueryBuilder(queryBuild, opts);
    }

    public searchByQueryBuilder<T>(queryBuilder: bodybuilder.Bodybuilder, opts: IElasticSearchParams): Promise<IElasticResult<T>> {

        let query = this._buildQuery(queryBuilder, opts);

        return this.search(opts.index, query);
    }

    public buildQuery(queryBuilder: bodybuilder.Bodybuilder, opts: IElasticSearchParams): object {
        return this._buildQuery(queryBuilder, opts);
    }

    private _buildQuery(queryBuilder: bodybuilder.Bodybuilder, opts: IElasticSearchParams): object {
        let {fields, pageSize, page, sort, filter, range} = opts;

        pageSize && queryBuilder.size(pageSize);
        page && queryBuilder.from((page - 1) * pageSize);

        if (fields && fields.length) {
            queryBuilder.rawOption("_source", {"includes": fields})
        }

        _.forEach(opts.sort, (item, key) => {
            queryBuilder.sort(item.field, item.dir)
        });

        _.forEach(filter, (item, key) => {

            if (item.type == "terms") {
                queryBuilder.andFilter("terms", item.field, item.value);
            } else {
                queryBuilder.andFilter("term", item.field, item.value);
            }


        });

        _.forEach(range, item => {

            queryBuilder.andFilter("range", item.field, {
                "gte": item.from,
                "lte": item.to,
            });
        });

        return queryBuilder.build();
    }

    public async deleteByTime(opts: { index: string, type: string, field: string, seconds: number, format: string }) {
        try {
            let params: RequestParams.DeleteByQuery<any> = {
                index: opts.index,
                conflicts: "proceed",
                wait_for_completion: false,
                body: {
                    "query": {
                        "bool": {
                            "must": {
                                "range": {
                                    [opts.field]: {
                                        "lte": moment().utc().subtract(opts.seconds, "seconds").format(opts.format)
                                    }
                                }
                            }
                        }
                    }
                }
            };

            let {body} = await this.client.deleteByQuery(params);

            return body;

        } catch (e) {

            this.logger.error(`failed to delete by time `, {e, opts});

            throw e;
        }
    }

    public async searchByParams<T>(index: string, params: { size?: number; sort?: { [index: string]: { order: "desc" | "asc" } }; query: any; }): Promise<IElasticResult<T>> {
        return this.search(index, params)
    }

    public async search<T>(index: string, params: { [index: string]: any }): Promise<IElasticResult<T>> {

        try {

            const response = await this.client.search({
                index: index,
                body: params
            });

            return {
                results: response.body.hits.hits.map(x => ({_id: x._id, ...x._source})),
                total: response.body.hits.total.value
            };
        } catch (e) {
            this.logger.error(`failed to to run elastic search`, {params: JSON.stringify(params), e});
            throw e;
        }
    }

    public async exists(index: string, id: string): Promise<boolean> {

        let doc = await this.client.exists({
            id: id,
            index: index,
        });

        return (doc.body as unknown) as boolean;
    }

    public async create<T extends object>(index: string, id: string, item: T) {
        await this.client.create({
            id: id,
            index: index,
            body: _.omit(item, ["_id", "id"])
        })
    }

    public async delete(index: string, id: string) {

        let isExists = await this.exists(index, id);

        if (!isExists) {
            return;
        }

        await this.client.delete({
            id: id,
            index: index,
        })
    }

    public async update<T extends object>(index: string, id: string, item: T) {

        let isExists = await this.exists(index, id);

        if (!isExists) {
            await this.create(index, id, item);
            return;
        }

        await this.client.update({
            id: id,
            index: index,
            body: {doc: _.omit(item, ["_id", "id"])}
        })
    }
}
