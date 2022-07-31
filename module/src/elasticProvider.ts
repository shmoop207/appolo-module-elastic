import {define, inject, singleton, alias, init} from "@appolo/inject";
import {Client} from '@elastic/elasticsearch';
import {IElasticResult, IElasticSearchParams, IOptions} from "./interfaces";
import * as bodybuilder from 'bodybuilder';
import {ILogger} from '@appolo/logger';
import {date} from '@appolo/date';
import {Arrays, Strings, Objects, Numbers} from '@appolo/utils';
import {MsearchMultiSearchItem} from "@elastic/elasticsearch/lib/api/types";

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

        let items: T[] = [];

        for (let i = 0; i < response.rows.length; i++) {
            let row = response.rows[i], dto: any = {};
            for (let j = 0; j < response.columns.length; j++) {

                let col = response.columns[j];

                dto[col.name] = row[j]
            }
            items.push(dto);
        }

        return items;

    }

    public async getById<T>(opts: { id: string, index: string, fields?: string[] }): Promise<T> {

        let dto: any = {
            index: opts.index,
            id: opts.id
        };

        if (opts.fields && opts.fields.length) {
            dto.body["_source"] = {"includes": opts.fields};
        }

        let {_source} = await this.client.get(dto);

        return _source as T
    }

    public searchByAll<T>(opts: IElasticSearchParams): Promise<IElasticResult<T>> {

        return this.searchByQueryBuilder(bodybuilder(), opts);
    }

    public async searchByAllMulti<T>(params: IElasticSearchParams[]): Promise<IElasticResult<T>[]> {

        let queries = params.map(item => [{index: item.index}, this._buildQuery(bodybuilder(), item)]);

        try {

            const response = await this.client.msearch<T>({
                body: Arrays.flat(queries)
            });


            let responses = response.responses as MsearchMultiSearchItem<T>[]

            return responses.map((res: MsearchMultiSearchItem) => ({
                total: Numbers.isNumber(res.hits.total) ? res.hits.total : res.hits.total.value,
                results: res.hits.hits.map<T>(x => Object.assign<any, any>({_id: x._id}, x._source))
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

        Arrays.forEach(opts.sort, (item, key) => {
            queryBuilder.sort(item.field, item.dir)
        });

        Arrays.forEach(filter, (item, key) => {

            queryBuilder.andFilter(item.type || "term", item.field, item.value);
        });

        Arrays.forEach(range, item => {

            queryBuilder.andFilter("range", item.field, {
                "gte": item.from,
                "lte": item.to,
            });
        });

        return queryBuilder.build();
    }

    public async deleteByTime(opts: { index: string, type: string, field: string, seconds: number, format?: string }) {
        try {

            let result = await this.client.deleteByQuery({
                index: opts.index,
                conflicts: "proceed",
                wait_for_completion: false,
                body: {
                    query: {
                        bool: {
                            must: {
                                range: {
                                    [opts.field]: {
                                        "lte": date().utc().subtract(opts.seconds, "seconds").format(opts.format)
                                    }
                                }
                            }
                        } as any,
                    }
                }
            });

            return result;

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

            const response = await this.client.search<T>({
                index: index,
                body: params
            });

            return {
                results: response.hits.hits.map(x => ({_id: x._id, ...x._source})),
                total: Numbers.isNumber(response.hits.total) ? response.hits.total : response.hits.total.value
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

        return (doc as unknown) as boolean;
    }

    public async create<T extends object>(index: string, id: string, item: T) {
        await this.client.create({
            id: id,
            index: index,
            body: Objects.omit(item, "_id" as any, "id")
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
            body: {doc: Objects.omit(item, "_id" as any, "id")}
        })
    }
}
