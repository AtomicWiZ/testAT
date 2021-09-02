import type { FilterableAttributeSlug } from '../../logic/ProductManager'
import type {
  DomainGroup,
  ScopeOption,
} from '../../logic/SearchManager'

import get from 'lodash/get'
import isEmpty from 'lodash/isEmpty'
import isArray from 'lodash/isArray'
import { Client } from '@elastic/elasticsearch'
import { IndexSettings, ProductModel } from './model/ProductModel'
import { config } from '../../config'
import { ClientErrorCode, DI, KobpError, ServerErrorCode } from 'kobp'
import { createHash } from 'crypto'
import { Deserialize, Serialize } from 'cerialize'
import { ReserveStock, Stock } from '../incart/models/products'
import { BrandEntity, BrandStoreEntity } from '../../db/entities/BrandEntity'
import { scriptCancelStock, scriptExpiredStock, scriptPaidStock, scriptReserveStock, scriptSaveBrands, scriptSaveProducts, scriptUpdateStock } from '../elasticsearch/es.script'
import { Lang } from '../../db/entities/CategoryEntity'
import { BrandsESFormat } from '../../logic/BrandManager'


type TermIndex = 'queryterms' | 'boostedterms'
export type SupportedIndex = 'products' | TermIndex | 'brands'

const PRODUCT_INDEX: SupportedIndex = 'products'
const BRAND_INDEX: SupportedIndex = 'brands'
const POPULAR_TERMS_INDEX: SupportedIndex = 'queryterms'
const BOOSTED_TERMS_INDEX: SupportedIndex = 'boostedterms'
const ALL_INDICES: SupportedIndex[] = ['products', 'queryterms', 'boostedterms']

/**
 * Example:
 * 
```json
{  "priceMin": {
    "value": 250
  },
  "priceMax": {
    "value": 272000
  },
  "brands": {
    "doc_count_error_upper_bound": 0,
    "sum_other_doc_count": 0,
    "buckets": [
      {
        "key": "bally",
        "doc_count": 57
      }
    ]
  },
  "categories": {
    "doc_count_error_upper_bound": 0,
    "sum_other_doc_count": 0,
    "buckets": []
  }
}
```
 */
interface ProductListAggregrationResult {
  priceMin: { value: number }
  priceMax: { value: number }
  brands: { buckets: { key: string, doc_count: number }[] }
  categorySlugs: { buckets: { key: string, doc_count: number }[] }
  colorSwatch: { buckets: { key: string, doc_count: number }[] }
}

interface ProductListSuggestResult {
  suggestionEn: { options: { _source: {title: {en: string } } }[] }[]
  suggestionTh: { options: { _source: {title: {th: string } } }[] }[]
}

interface BrandListSuggestResult {
  suggestion: { options: { _source: {id: string } }[] }[]
}

// export interface BrandsTerms {
//   id: string
//   title: string
//   logoUrl: string
// }

export const isSupportedIndex = (o: string): o is SupportedIndex => {
  return ALL_INDICES.findIndex((k) => `${k}` === `${o}`) >= 0
}

export interface ListProductsOptions extends ScopeOption {
  // Search
  keyword?: string
  // Pagination
  nextToken?: string
  size: number
  categories?: Array<string>
  minPrice?: number
  maxPrice?: number
  brands?: Array<string>
  sortBy?: string
  skus?: Array<string>
  checkStatus?: boolean
  color?: Array<string>
  pageSize?: number
  // TODO: Additional search options should be added here.
}

export interface ListProductsResult<I extends ProductModel> {
  /**
   * Products to be returned
   */
  items: I[]
  /**
   * Next page to support pagination
   */
  nextToken: string | null
  /**
   * Total information
   */
  total: {
    value: number
    isEstimate: boolean
  }

  filterableAttributes: FilterableAttribute

  suggestionProduct: string[]
}

export interface ListBrandsResult<I extends BrandsESFormat> {
  /**
   * Products to be returned
   */
  items: I[]
  /**
   * Next page to support pagination
   */
  nextToken: string | null
  /**
   * Total information
   */
  total: {
    value: number
    isEstimate: boolean
  }
  suggestionBrand: BrandEntity[]
}

export interface FilterableAttribute {
  brands: { brandId: string, count: number }[]
  categories: { slug: string, count: number }[]
  price: {
    priceMin: number
    priceMax: number
  }
  colorSwatch: { code: string, label: Lang, value?: string }[]
}

// Uniformly handle elastic search error.
const _logElasticSearchError = (error: Error & { meta?: any }): void => {
  if (error.meta) {
    console.error('ES ERR', JSON.stringify(error.meta.body))
  }
}

const _throwElasticSearchError = (error: Error & { meta?: any }): never => {
  if (error.meta) {
    console.error('ES ERR', JSON.stringify(error.meta.body))
  }
  throw KobpError.fromServer(ServerErrorCode.badGateway, 'ElasticSearch encountered an error', _normalizeESErrorMessage(error))
}

const _normalizeESErrorMessage = (error: Error) => {
  let normalizedErrorMessage = error && error.message
  const esErrorNode = get(error, 'meta.body.error', null)
  if (esErrorNode) {
    const typeWithReason = (o: any): string => {
      return [o.type, o.reason]
        .filter(Boolean)
        .join(': ')
    }
    if (esErrorNode.root_cause && isArray(esErrorNode.root_cause)) {
      normalizedErrorMessage = esErrorNode.root_cause.map(typeWithReason).join(', ')
    } else if (esErrorNode.caused_by && typeWithReason(esErrorNode.caused_by)) {
      normalizedErrorMessage = typeWithReason(esErrorNode.caused_by)
    }
  }
  return normalizedErrorMessage
}

export class ElasticsearchService {

  private static _instance: ElasticsearchService

  public static getInstance(): ElasticsearchService {
    // const key = `${config.url}:${config.port}`
    return this._instance || (this._instance = new ElasticsearchService())
  }

  private _client: Client

  private constructor() {
    this._client = new Client(config.elasticSearchConfig)
  }
  
  /**
   * Put searched term into Elasticsearch so that we can rank them later on.
   * @param domain 
   * @param typed 
   */
  public async trackUserSearchedKeywords(scope: ScopeOption, typed: string): Promise<void> {
    const hasher = createHash('md5')
    const domain = this.fromScopeToDomain(scope)
    const id = hasher.update(`${domain}/${typed}`.toLowerCase()).digest('base64')
    await this._client.update({
      index: POPULAR_TERMS_INDEX,
      id,
      body: {
        script: {
          source: 'ctx._source.hit+=1',
        },
        upsert: {
          q: typed.toLowerCase(),
          domain,
          hit: 1,
        }
      },
      retry_on_conflict: 3,
    }).catch(_logElasticSearchError)
  }

  /**
   * Put configurable search term into Elasticsearch so that we can use this to supply to user when asked from particular domain.
   * @param scope 
   * @param keyword 
   * @param score 
   */
  public async updateDomainBoostedKeywords(scope: ScopeOption, keyword: string, score: number): Promise<void> {
    const hasher = createHash('md5')
    const domain = this.fromScopeToDomain(scope)
    console.log('DOMAIN', domain)
    const id = hasher.update(`${domain}/${keyword}`.toLowerCase()).digest('base64')
    await this._client.update({
      index: BOOSTED_TERMS_INDEX,
      id,
      body: {
        script: {
          source: 'ctx._source.score = params.score;',
          params: {
            score,
          }
        },
        upsert: {
          q: keyword.toLowerCase(),
          domain,
          score,
        }
      },
      retry_on_conflict: 3,
    }).catch(_throwElasticSearchError)
  }

  public async getDomainBoostedKeywords(scope: ScopeOption): Promise<{ keyword: string, score: number }[]> {
    const domain = this.fromScopeToDomain(scope)
    const result = await this._client.search({
      index: BOOSTED_TERMS_INDEX,
      body: {
        size: 1000,
        query: {
          bool: {
            filter: [
              { term: { domain } },
            ],
          }
        }
      }
    }).catch(_throwElasticSearchError)
    const arr = get(result, 'body.hits.hits', [])
    return arr.map((o: any) => o._source) // ES Result
  }

  /**
   * Find the popular keywords based on typed information. (under given domain)
   * 
   * @param domainGroup 
   * @param domainScope 
   * @param typed 
   */
  public async popularTerms(scope: ScopeOption, typed?: string): Promise<string[]> {
    return this._queryTerms('queryterms', scope, typed)
  }

  public async boostedTerms(scope: ScopeOption, typed?: string): Promise<string[]> {
    return this._queryTerms('boostedterms', scope, typed)
  }

  private async _queryTerms(index: TermIndex, scope: ScopeOption, typed?: string): Promise<string[]> {
    const domain = this.fromScopeToDomain(scope)
    let sortNode = []
    sortNode.push({
      _score: 'desc'
    })
    if (index == 'boostedterms') {
      sortNode.push({
        score: 'desc'
      })
    }
    else if (index == 'queryterms') {
      sortNode.push({
        hit: 'desc'
      })
    }
    try {
      const { body } = await this._client.search({
        index,
        body: {
          size: 10,
          query: {
            bool: {
              should: [
                typed && { term: { q: typed } } || undefined,
              ].filter(Boolean),
              filter: [
                { term: { domain: domain } },
              ]
            },
          },
          ...(
            sortNode && sortNode.length > 0 && ({
              sort: sortNode
            })
          )
        },
      })
      const hits: any[] = get(body, 'hits.hits') || []
      console.log('Body', JSON.stringify(hits))
      return hits.map((o) => o._source.q)
    } catch (error: any) {
      _throwElasticSearchError(error) /* this will throw error */
      throw error // this will never reached
    }
  }

  public async deletePopularTerms(scope: ScopeOption, keywords: string[]): Promise<number> {
    return this._deleteTerms('queryterms', scope, keywords)
  }

  public async deleteBoostedTerms(scope: ScopeOption, keywords: string[]): Promise<number> {
    return this._deleteTerms('boostedterms', scope, keywords)
  }

  private async _deleteTerms(index: TermIndex, scope: ScopeOption, keywords: string[]): Promise<number> {
    const domain = this.fromScopeToDomain(scope)
    const queryMust = []
    const queryMatch: any[] = []
    try {
      if (keywords && keywords.length) {
        for (const k of keywords) {
          queryMatch.push({
            match: {
              q: k
            }
          }) 
        }
        queryMust.push({
          bool: {
            should: queryMatch
          }})
        queryMust.push({
          match: {
            domain: domain
          }
        })
        const { body } = await this._client.deleteByQuery({
          index: index,
          body: {
            query: {
              bool: {
                ...(
                  queryMust && queryMust.length > 0 && ({
                    must: queryMust
                  })
                )
              }
            }
          }
        })
        return get(body, 'deleted') || 0
      }
      return 0
    } catch (error: any) {
      _throwElasticSearchError(error) /* this will throw error */
      throw error // this will never reached
    }
  }

  /** Sync Products by brands from Incart to Elasticsearch
   * @param data
   * @returns <-- success case
   */
  public async saveProducts(data: Array<ProductModel>): Promise<boolean> {
    // map data format     
    const body = data.flatMap((productModel) => {
      const rawDoc = Serialize(productModel)
      return [
        { 
          update: { 
            _id : rawDoc.sku,
            _index : PRODUCT_INDEX,
          }
        },
        { 
          script: {
            source: scriptSaveProducts, 
            lang: "painless", 
            params: rawDoc,
          }, 
          upsert: rawDoc
        }
      ]
    })
    try {
      // Parse update results.
      await this._client.bulk({
        refresh: true,
        body,
      })
      // Uncomment to debug update results.
      // console.log('RESULT>>', result.body.items.map((o: any) => o.update?.error?.caused_by), result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }

    return true
  }

  /** Sync Brands from Database to Elasticsearch
   * @param brands
   * @returns <-- success case
   */
   public async syncBrands(brands: BrandsESFormat[]): Promise<boolean> {
    // map data format     
    const body = brands.flatMap((brand) => {
      const rawDoc: BrandEntity = Serialize(brand)
      console.log(rawDoc)
      return [
        { 
          update: { 
            _id : rawDoc.id,
            _index : BRAND_INDEX,
          }
        },
        { 
          script: {
            source: scriptSaveBrands, 
            lang: "painless", 
            params: rawDoc,
          }, 
          upsert: rawDoc
        }
      ]
    })
    try {
      // Parse update results.
      await this._client.bulk({
        refresh: true,
        body,
      })
      // Uncomment to debug update results.
      // console.log('RESULT>>', result.body.items.map((o: any) => o.update?.error?.caused_by), result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }

    return true
  }

  /**
   * List products from ElasticSearch service.
   *
   * @param options options of ListProductsOptions
   * @param filterableAttributeSlugs list of attributes to extract as fitlerable attributes.
   * @returns 
   */
  public async listProducts(options: ListProductsOptions, filterableAttributeSlugs: FilterableAttributeSlug[]): Promise<ListProductsResult<ProductModel>> {
    try {
      // Create filterable clause
      const queryBoolFilters: { [key: string]: any }[] = [
        // { term: { status: true } },
      ]
      let queryShould = []
      let queryMust = []
      let sortNode = []
      let aggBrandId = {}
      let aggCategory = {}
      let aggColorSwatch = {}
      // Filter by skus
      if (options.skus && options.skus.length) {
        let shouldArr: any[] = []
        options.skus.forEach(item => {
          let nestedArr = [
            {
              nested: {
                path: "byStore",
                query: {
                  bool: {
                    must: {
                      match: {
                        "byStore.sku": item.toLowerCase()
                      }
                    }
                  }
                }
              }
            },
            {
              nested: {
                path: "byStore.children",
                query: {
                  bool: {
                    must: {
                      match: {
                        "byStore.children.sku": item.toLowerCase()
                      }
                    }
                  }
                }
              }
            }
          ]
          shouldArr = shouldArr.concat(nestedArr)
        })
        queryMust.push({ 
          bool: {
            should: shouldArr,
            minimum_should_match: 1  
          }
        })
      } 
      // if (options.checkStatus) {
      //   // Filter enabled product from finalStatus
      //   queryMust.push({ match: { finalStatus: 'enabled' } })
      //   // Filter in stock product from stockStatus
      //   queryMust.push({ match: { stockStatus: 'in-stock' } })
      // }
      let suggestProducts = {}
      if (options.keyword) {
        queryShould.push({ match_phrase_prefix: { 'title.th': options.keyword } })
        queryShould.push({ match_phrase_prefix: { 'title.en': options.keyword } })
        //Adding Suggestion for multi language
        suggestProducts = {
          suggestionEn : {
            prefix : options.keyword,
            completion : {
              field : 'titleSuggest.en',
              size: 5
            }
          },
          suggestionTh : {
            prefix : options.keyword,
            completion : {
              field : 'titleSuggest.th',
              size: 5
            }
          }
        }
      }
      // Filter by categories
      if (options.categories && options.categories.length) {
        options.categories.forEach(item => {
          queryShould.push({ match: { categorySlugs: item } })
        })
      }
      // Filter by Color Swatch
      if (options.color && options.color.length) {
        const shouldArr: any[] = []
        options.color.forEach(item => {
          shouldArr.push({ match: { colorSwatchs: item } })
        })
        queryMust.push({ 
          bool: {
            should: shouldArr,
            minimum_should_match: 1  
          }
        })
      }
      // Filter by price range
      if (options.minPrice || options.maxPrice) {
        queryMust.push({ range: { actualMinPrice: { gte: options.minPrice } } })
        queryMust.push({ range: { actualMinPrice: { lte: options.maxPrice } } })
      }
      // Filter by mallId
      if (options.mallId) {
        queryBoolFilters.push({
          nested: {
            path: 'byStore',
            query: {
              bool: {
                must: {
                  match: {
                    'byStore.mallId': options.mallId
                  }
                }
              }
            }
          } 
        })
      }
      aggBrandId = {
        brands: {
          terms: {
            field: 'annotatedBrand',
            order: { _count: 'desc' },
          }
        }
      }
      aggCategory = {
        categorySlugs: {
          terms: {
            field: 'categorySlugs',
            order: { _count: 'desc' },
          }
        }
      }
      // Filter by brandId
      if (options.brandId) {
        queryBoolFilters.push({ term: { annotatedBrand: options.brandId }})
        aggBrandId = {}
      }
      // Filter by category
      if (options.category) {
        queryBoolFilters.push({ term: { categorySlugs: options.category }})
        aggCategory = {}
      }
      // Filter only when filterableAttributeSlugs signify that the colorSwatch is needed.
      if (filterableAttributeSlugs.indexOf('colorSwatch') !== -1) {
        aggColorSwatch = {
          colorSwatch: {
            terms: {
              field: 'colorSlugs',
              order: { _count: 'desc' },
            }
          }
        }
      }
      // Filter by brands
      if (options.brands && options.brands.length) {
        queryBoolFilters.push({ terms: { annotatedBrand: options.brands }})
      }
      sortNode.push({ _score: "desc" })
      if (options.sortBy) {
        const sortDir: 'asc' | 'desc' = options.sortBy.charAt(0) === '-' ? 'desc' : 'asc'
        if (/createdAt/i.test(options.sortBy)) {
          options.sortBy = 'createdAt'
        } else if (/price/i.test(options.sortBy)) {
          options.sortBy = 'actualMinPrice'
        } else if (/discountPercent/i.test(options.sortBy)) {
          options.sortBy = 'discountPercent'
        } else if (/id/i.test(options.sortBy)) {
          options.sortBy = 'id'
        } else {
          throw KobpError.fromUserInput(ClientErrorCode.badRequest, `Unknown sort by option: ${options.sortBy}`)
        }
        sortNode.push({ [options.sortBy]: sortDir })
      }
      sortNode.push({ id: 'asc' })
      const queryBoolNode = {
        // Include should clause if required
        ...(
          queryShould && queryShould.length > 0 && ({
            should: queryShould,
            minimum_should_match: 1,
          })
        ),
        // Include must clause if required
        ...(
          queryMust && queryMust.length > 0 && ({
            must: queryMust
          })
        ),
        // Include filter clause
        ...(
          queryBoolFilters && queryBoolFilters.length > 0 && ({
            filter: queryBoolFilters || undefined,
          })
        ),
      }

      const queryNode = isEmpty(queryBoolNode)
        ? { query_string: { query: '*' } } // Fallback Empty Search
        : { bool: queryBoolNode } // Conditional Search

      const requestBody: Record<string, any> = {
        size: options.size,
        query: queryNode,
        ...(
          suggestProducts && !isEmpty(suggestProducts) && ({
            suggest: suggestProducts
          })
        ),
        // Always sort by relevancy
        sort: sortNode,
        aggs: {
          ...(aggBrandId),
          priceMax: {
            max: {
              field: 'priceMax',
            },
          },
          priceMin: {
            min: {
              field: 'priceMin',
            }
          },
          ...(aggCategory),
          ...(aggColorSwatch)
          // TODO: Add other aggregration node here.
        },
      }
      if (options.nextToken) {
        requestBody['search_after'] = JSON.parse(Buffer.from(options.nextToken, 'base64').toString('utf8'))
      }
      console.log(JSON.stringify(requestBody), ' requestBody2')
      const { body } = await this._client.search({
        index: PRODUCT_INDEX,
        body: requestBody,
      })

      // reply nextToken if valid
      let nextToken: string | null = null
      const hits: any[] = get(body, 'hits.hits') || []
      const totals: { value: number, relation: string } = get(body, 'hits.total', {})
      const items: ProductModel[] = hits.map((item, index, arr) => {
        // Collect last item as nextToken,
        // and lastItem equals to page size so that we don't need frontend to call us again on next empty page.
        // console.log('ITEMS', JSON.stringify(item))
        if (index === arr.length - 1 && arr.length === options.size) {
          if (!item.sort) {
            throw KobpError.fromServer(ServerErrorCode.badGateway, 'Sort is missing from query result!')
          }
          nextToken = Buffer.from(JSON.stringify(item.sort), 'utf8').toString('base64') || null
        }
        return Deserialize(item._source, ProductModel)
      })
      // additional result over filterable attributes
      // const filterableAttributes: FilterablseAttribute = {}
      const aggregrations: ProductListAggregrationResult | null = get(body, 'aggregations', null)
      const suggestions: ProductListSuggestResult | null = get(body, 'suggest', null)
      const filterableAttributes: FilterableAttribute = {
        brands: [],
        categories: [],
        price: {
          priceMax: 999999,
          priceMin: 0,
        },
        colorSwatch: []
      }
      if (aggregrations) {
        filterableAttributes.brands = (aggregrations?.brands?.buckets || []).map((o) => ({ brandId: o.key, count: o.doc_count }))
        filterableAttributes.categories = (aggregrations?.categorySlugs?.buckets || []).map((o) => ({ slug: o.key, count: o.doc_count }))
        filterableAttributes.price = {
          priceMax: aggregrations.priceMax.value,
          priceMin: aggregrations.priceMin.value,
        }
        filterableAttributes.colorSwatch = (aggregrations?.colorSwatch?.buckets || [])
        .map((o) => {
          const colorLabel: string[] = o.key.split('__')
          return { 
            code: colorLabel[0], 
            label: { 
              en: colorLabel[1], 
              th: colorLabel[2] 
            } 
          }
        })
      }
      let suggestionProduct: string[] = []
      if (suggestions) {
        suggestionProduct = suggestions?.suggestionEn[0].options.length ? (suggestions?.suggestionEn[0].options || []).map(o => o._source.title.en) : (suggestions?.suggestionTh[0].options || []).map(o => o._source.title.th)
      }
      return {
        nextToken,
        total: {
          value: totals && totals.value || items.length,
          isEstimate: totals && totals?.relation !== 'eq',
        },
        filterableAttributes,
        suggestionProduct,
        items,
      }
    } catch (error: any) {
      _throwElasticSearchError(error)
      throw error
    }
  }

  /**
   * List brands from ElasticSearch service.
   * @param options options of ListProductsOptions
   * @returns 
   */
  public async listBrands(options: ListProductsOptions): Promise<ListBrandsResult<BrandsESFormat>> {
    try {
      let queryMatch = {}
      let suggestMatch = {}
      let searchAfter = {}
      if (options.keyword) {
        queryMatch = {
          match_phrase_prefix: {
            title: options.keyword
          }
        }
        suggestMatch = {
          suggestion: {
            prefix: options.keyword,
            completion: {
              field: 'titleSuggest',
              size: 5
            }
          }
        }
      }
      else {
        queryMatch = {
          match_all: {}
        }
      }
      if (options.nextToken) {
        searchAfter = JSON.parse(Buffer.from(options.nextToken, 'base64').toString('utf8'))
      }
      const requestBody = {
        index: BRAND_INDEX,
        body: {
          size: 24,
          ...(
            queryMatch && !isEmpty(queryMatch) && ({
              query: queryMatch
            })
          ),
          ...(
            suggestMatch && !isEmpty(suggestMatch) && ({
              suggest: suggestMatch
            })
          ),
          sort: [
            {
              _score: 'desc'
            },
            {
              id: 'asc'
            }
          ],
          ...(
            searchAfter && !isEmpty(searchAfter) && ({
              search_after: searchAfter
            })
          ),
        }
      }
      const { body } = await this._client.search(requestBody)
      // reply nextToken if valid
      let nextToken: string | null = null
      const hits: any[] = get(body, 'hits.hits') || []
      const totals: { value: number, relation: string } = get(body, 'hits.total', {})
      const items: BrandsESFormat[] = hits.map((item, index, arr) => {
        // Collect last item as nextToken,
        // and lastItem equals to page size so that we don't need frontend to call us again on next empty page.
        // console.log('ITEMS', JSON.stringify(item))
        if (index === arr.length - 1 && arr.length === options.size) {
          if (!item.sort) {
            throw KobpError.fromServer(ServerErrorCode.badGateway, 'Sort is missing from query result!')
          }
          nextToken = Buffer.from(JSON.stringify(item.sort), 'utf8').toString('base64') || null
        }
        return item._source
      })
      const suggestions: BrandListSuggestResult | null = get(body, 'suggest', null)
      let suggestion: string[] = []
      if (suggestions) {
        suggestion = (suggestions?.suggestion[0].options || []).map(o => o._source.id)
      }
      const brandsList = await DI.em.find(BrandEntity, {
        id: suggestion,
      }, ['stores', 'stores.mall'])
      return {
        nextToken,
        total: {
          value: totals && totals.value || items.length,
          isEstimate: totals && totals?.relation !== 'eq',
        },
        suggestionBrand: brandsList,
        items
      }
    } catch (error: any) {
      _throwElasticSearchError(error) /* this will throw error */
      throw error // this will never reached
    }
  }

  private fromScopeToDomain(scope: ScopeOption): string {
    let dg: DomainGroup = 'global'
    let ds = ''
    if (scope.brandId) {
      dg = 'brand'
      ds = scope.brandId
    }
    // TODO: Detect other domains here.
    return [dg, ds].filter(Boolean).join(':')
  }

  public async getProductBySku(sku: string): Promise<ProductModel> {
    try {
      const { body } = await this._client.get({
        index: PRODUCT_INDEX,
        type: '_doc',
        id: sku
      })
      return body._source
    } catch (error) {
      throw KobpError.fromUserInput(ClientErrorCode.notFound, 'Resource not found')
    }
  }

  public async deleteProductBySku(sku: string): Promise<boolean> {
    try {
      await this._client.delete({
        index: PRODUCT_INDEX,
        id: sku
      })
      return true
    } catch (error) {
      throw KobpError.fromUserInput(ClientErrorCode.notFound, 'Error Delete')
    }
  }

  public async updateStocks(requestBody: Stock[], brandStore: BrandStoreEntity): Promise<boolean> {
    // map data format     
    const body = requestBody.flatMap((doc) => [
      { 
        update: { 
          _id : doc.indexId,
          _index : PRODUCT_INDEX,
        }
      },
      { 
        script: {
          source: scriptUpdateStock, 
          lang: "painless", 
          params: doc,
        }
      }
    ])
    try {
      const result = await this._client.bulk({
        refresh: true,
        body,
      })
      console.log('RESULT>>', result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }
    return true
  }

  public async reserveStocks(requestBody: ReserveStock[]): Promise<boolean> {
    // map data format     
    const body = requestBody.flatMap((doc) => [
      { 
        update: { 
          _id : doc.indexId,
          _index : PRODUCT_INDEX,
        }
      },
      { 
        script: {
          source: scriptReserveStock, 
          lang: "painless", 
          params: doc,
        }
      }
    ])
    try {
      const result = await this._client.bulk({
        refresh: true,
        body,
      })
      console.log('RESULT>>', result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }
    return true
  }

  public async paidStocks(requestBody: ReserveStock[]): Promise<boolean> {
    // map data format     
    const body = requestBody.flatMap((doc) => [
      { 
        update: { 
          _id : doc.indexId,
          _index : PRODUCT_INDEX,
        }
      },
      { 
        script: {
          source: scriptPaidStock, 
          lang: "painless", 
          params: doc,
        }
      }
    ])
    try {
      const result = await this._client.bulk({
        refresh: true,
        body,
      })
      console.log('RESULT>>', result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }
    return true
  }

  public async expiredStocks(requestBody: ReserveStock[]): Promise<boolean> {
    // map data format     
    const body = requestBody.flatMap((doc) => [
      { 
        update: { 
          _id : doc.indexId,
          _index : PRODUCT_INDEX,
        }
      },
      { 
        script: {
          source: scriptExpiredStock, 
          lang: "painless", 
          params: doc,
        }
      }
    ])
    try {
      const result = await this._client.bulk({
        refresh: true,
        body,
      })
      console.log('RESULT>>', result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }
    return true
  }

  public async cancelStocks(requestBody: ReserveStock[]): Promise<boolean> {
    // map data format     
    const body = requestBody.flatMap((doc) => [
      { 
        update: { 
          _id : doc.indexId,
          _index : PRODUCT_INDEX,
        }
      },
      { 
        script: {
          source: scriptCancelStock, 
          lang: "painless", 
          params: doc,
        }
      }
    ])
    try {
      const result = await this._client.bulk({
        refresh: true,
        body,
      })
      console.log('RESULT>>', result.body.items[0].update.error)
    } catch (error: any) {
      _throwElasticSearchError(error)
    }
    return true
  }

  /**
   * configure existing index.
   * 
   * @param targetIndex - target index to update settings
   * @param settings - settings to patch
   * @returns 
   */
  public async configIndex(targetIndex: SupportedIndex, indexSettings: IndexSettings, destroyExistingIndex: boolean = true): Promise<IndexSettings> {
    try {
      const indexExists = await this.isIndexExists(targetIndex)
      if (indexExists) {
        if (destroyExistingIndex) {
          console.log('Deleting index')
          await this._client.indices.delete({
            index: targetIndex,
          })
        }
        throw KobpError.fromServer(ServerErrorCode.internalServerError, `Index: ${targetIndex} already exists.`)
      }
      // Create it!
      await this._client.indices.create({
        index: targetIndex,
      })
      // close index connection
      await this._client.indices.close({
        index: targetIndex,
      })
      // put setting & put mapping
      await this.putSettings(targetIndex, indexSettings.settings)
      await this.putMapping(targetIndex, indexSettings.mappings.properties)
      return indexSettings
    } catch (error: any) {
      throw KobpError.fromServer(ServerErrorCode.badGateway, `Unable to perform Setting Index on ES: ${_normalizeESErrorMessage(error)}`)
    } finally {
      await this._client.indices.open({
        index: targetIndex
      })
    }
  }

  private async isIndexExists(indexName: SupportedIndex): Promise<boolean> {
    const { body } = await this._client.indices.exists({
      index: indexName
    })
    return Boolean(body)
  }

  private async putSettings(index: SupportedIndex, settings: any): Promise<void> {
    try {
      await this._client.indices.putSettings({
        index,
        body: {
          settings,
        }
      })
    } catch (error: any) {
      throw KobpError.fromServer(ServerErrorCode.badGateway, _normalizeESErrorMessage(error))
    }
  }
  
  private async putMapping(index: SupportedIndex, properties: object): Promise<void> {
    try {
      await this._client.indices.putMapping({
        index,
        body: {
          properties: properties || {}
        }
      })
    } catch (error: any) {
      throw KobpError.fromServer(ServerErrorCode.badGateway, _normalizeESErrorMessage(error))
    }
  }
}
