export interface IOptions {
    id?: string
    connection: string
    requestTimeout?: number
}

export enum ElasticSortDir {
    Desc = "desc",
    Asc = "asc"
}

export interface IElasticResult<T> {
    results: T[],
    total: number
}

export interface IElasticSearchParams {
    fields?: string[],
    index: string,
    page?: number,
    pageSize?: number
    sort?: { field: string, dir: ElasticSortDir }[]
    filter?: { field: string, type?: "term" | "terms" | "match_phrase_prefix" | "match" | "match_phrase" | "fuzzy" | "prefix", value: string | number | boolean | any[] }[]
    range?: { from?: string | number, to?: string | number, field: string }[]
}
