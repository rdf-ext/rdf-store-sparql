import { deepStrictEqual, rejects, strictEqual, throws } from 'node:assert'
import toNT from '@rdfjs/to-ntriples'
import { describe, it } from 'mocha'
import rdf from 'rdf-ext'
import eventToPromise from 'rdf-utils-stream/eventToPromise.js'
import SparqlStore from '../index.js'
import * as ns from './support/namespaces.js'
import virtualEndpoint from './support/virtualEndpoint.js'

const example = {}

example.dataset = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object, ns.ex.graph)])
example.datasetDefaultGraph = rdf.dataset(example.dataset, rdf.defaultGraph())

describe('store-sparql', () => {
  describe('constructor', () => {
    it('should throw an error if no endpointUrl is given', () => {
      return throws(() => {
        new SparqlStore({}) // eslint-disable-line no-new
      })
    })

    it('should use the given endpointUrl in the SPARQL client instance', () => {
      const endpointUrl = 'http://example.org/sparql'

      const store = new SparqlStore({ endpointUrl })

      strictEqual(store.client.endpointUrl, endpointUrl)
    })

    it('should use endpointUrl as updateUrl if updateUrl is not given', () => {
      const endpointUrl = 'http://example.org/sparql'

      const store = new SparqlStore({ endpointUrl })

      strictEqual(store.client.updateUrl, endpointUrl)
    })
  })

  describe('.match', () => {
    it('should use the endpoint URL', () => {
      const id = '/match/url'

      const { result, store } = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const stream = store.match()

      return rdf.dataset().import(stream).then(() => {
        strictEqual(result.touched, true)
      })
    })

    it('should use CONSTRUCT query', () => {
      const id = '/match/construct'
      const query = 'CONSTRUCT{?s?p?o}{GRAPH?g{?s?p?o}}'

      const { result, store } = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const stream = store.match()

      return rdf.dataset().import(stream).then(() => {
        deepStrictEqual(result.queries, [query])
      })
    })

    it('should stream the result', () => {
      const id = '/match/stream'

      const { store } = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const stream = store.match()

      return rdf.dataset().import(stream).then(dataset => {
        strictEqual(example.datasetDefaultGraph.toCanonical(), dataset.toCanonical())
      })
    })

    it('should support filters', async () => {
      const id = '/match/construct-filter'
      const query = `CONSTRUCT{${toNT(ns.ex.subject)}${toNT(ns.ex.predicate)}?o}{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)}${toNT(ns.ex.predicate)}?o}}`

      const { result, store } = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const stream = store.match(ns.ex.subject, ns.ex.predicate, null, ns.ex.graph)
      await rdf.dataset().import(stream)

      deepStrictEqual(result.queries, [query])
    })

    it('should should support Default Graph filter', async () => {
      const id = '/match/construct-filter-default'
      const query = `CONSTRUCT{${toNT(ns.ex.subject)}${toNT(ns.ex.predicate)}?o}{${toNT(ns.ex.subject)}${toNT(ns.ex.predicate)}?o}`

      const { result, store } = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const stream = store.match(ns.ex.subject, ns.ex.predicate, null, rdf.defaultGraph())
      await rdf.dataset().import(stream)

      deepStrictEqual(result.queries, [query])
    })

    it('should handle errors', async () => {
      const id = '/match/error'

      const { store } = virtualEndpoint({
        id,
        statusCode: 500
      })

      const stream = store.match()

      await rejects(() => rdf.dataset().import(stream))
    })
  })

  describe('.import', () => {
    it('should use update URL', async () => {
      const id = '/import/url'

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.import(example.dataset.toStream())
      await eventToPromise(stream)

      strictEqual(result.touched, true)
    })

    it('should use INSERT DATA { GRAPH <...> {...} } query', async () => {
      const id = '/import/insert'
      const query = `INSERT DATA{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.import(example.dataset.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, [query])
    })

    it('should use INSERT DATA { ... } query for Default Graph', async () => {
      const id = '/import/insert-default-graph'
      const query = `INSERT DATA{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.import(example.datasetDefaultGraph.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, [query])
    })

    it('should split queries if content is to long', async () => {
      const id = '/import/split-length'
      const queries = [
        `INSERT DATA{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} "object1" .}}`,
        `INSERT DATA{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} "object2" .}}`
      ]

      const exampleDataset = rdf.dataset([
        rdf.quad(ns.ex.subject, ns.ex.predicate, rdf.literal('object1'), ns.ex.graph),
        rdf.quad(ns.ex.subject, ns.ex.predicate, rdf.literal('object2'), ns.ex.graph)
      ])

      const { result, store } = virtualEndpoint({
        count: 2,
        id,
        maxQueryLength: 120,
        method: 'POST'
      })

      const stream = store.import(exampleDataset.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, queries)
    })

    it('should split queries on new graph', async () => {
      const id = '/import/split-graph'
      const queries = [
        `INSERT DATA{GRAPH${toNT(ns.ex.graph1)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}}`,
        `INSERT DATA{GRAPH${toNT(ns.ex.graph2)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}}`
      ]

      const exampleDataset = rdf.dataset([
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object, ns.ex.graph1),
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object, ns.ex.graph2)
      ])

      const { result, store } = virtualEndpoint({ method: 'POST', id, count: 2 })

      const stream = store.import(exampleDataset.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, queries)
    })

    it('should handle errors', async () => {
      const id = '/import/error'

      const { store } = virtualEndpoint({ id, method: 'POST', statusCode: 500 })

      const stream = store.import(example.dataset.toStream())

      await rejects(() => eventToPromise(stream))
    })
  })

  describe('.remove', () => {
    it('should use update URL', async () => {
      const id = '/remove/url'

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.remove(example.dataset.toStream())
      await eventToPromise(stream)

      strictEqual(result.touched, true)
    })

    it('should use DELETE DATA {GRAPH<...>} query', async () => {
      const id = '/remove/delete'
      const query = `DELETE DATA{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.remove(example.dataset.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, [query])
    })

    it('should use DELETE DATA {} query for Default Graph', async () => {
      const id = '/remove/delete-default-graph'
      const query = `DELETE DATA{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.remove(example.datasetDefaultGraph.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, [query])
    })

    it('should split queries if content is to long', async () => {
      const id = '/remove/split-length'
      const queries = [
        `DELETE DATA{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} "object1" .}}`,
        `DELETE DATA{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} "object2" .}}`
      ]

      const exampleDataset = rdf.dataset([
        rdf.quad(ns.ex.subject, ns.ex.predicate, rdf.literal('object1'), ns.ex.graph),
        rdf.quad(ns.ex.subject, ns.ex.predicate, rdf.literal('object2'), ns.ex.graph)
      ])

      const { result, store } = virtualEndpoint({
        count: 2,
        id,
        maxQueryLength: 120,
        method: 'POST'
      })

      const stream = store.remove(exampleDataset.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, queries)
    })

    it('should split queries on new graph', async () => {
      const id = '/remove/split-graph'
      const queries = [
        `DELETE DATA{GRAPH${toNT(ns.ex.graph1)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}}`,
        `DELETE DATA{GRAPH${toNT(ns.ex.graph2)}{${toNT(ns.ex.subject)} ${toNT(ns.ex.predicate)} ${toNT(ns.ex.object)} .}}`
      ]

      const exampleDataset = rdf.dataset([
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object, ns.ex.graph1),
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object, ns.ex.graph2)
      ])

      const { result, store } = virtualEndpoint({ method: 'POST', id, count: 2 })

      const stream = store.remove(exampleDataset.toStream())
      await eventToPromise(stream)

      deepStrictEqual(result.queries, queries)
    })

    it('should handle errors', async () => {
      const id = '/remove/error'

      const { store } = virtualEndpoint({ id, method: 'POST', statusCode: 500 })

      const stream = store.remove(example.dataset.toStream())

      await rejects(() => eventToPromise(stream))
    })
  })

  describe('.removeMatches', () => {
    it('should use update URL', async () => {
      const id = '/remove-matches/url'

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.removeMatches()
      await eventToPromise(stream)

      strictEqual(result.touched, true)
    })

    it('should use DELETE WHERE {GRAPH?g{...}} query', async () => {
      const id = '/remove-matches/delete-where'
      const query = 'DELETE WHERE{GRAPH?g{?s?p?o}}'

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.removeMatches()
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should use DELETE WHERE {GRAPH<...>{...}} query for Named Graph', async () => {
      const id = '/remove-matches/delete-where-graph'
      const query = `DELETE WHERE{GRAPH${toNT(ns.ex.graph)}{${toNT(ns.ex.subject)}?p?o}}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.removeMatches(ns.ex.subject, null, null, ns.ex.graph)
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should use DELETE WHERE {...} query for Default Graph', async () => {
      const id = '/remove-matches/delete-default-graph'
      const query = `DELETE WHERE{${toNT(ns.ex.subject)}?p?o}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.removeMatches(ns.ex.subject, null, null, rdf.defaultGraph())
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should use CLEAR GRAPH <...> query for graph only filter', async () => {
      const id = '/remove-matches/delete-graph-only'
      const query = `CLEAR GRAPH${toNT(ns.ex.graph)}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.removeMatches(null, null, null, ns.ex.graph)
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should support filters', async () => {
      const id = '/remove-matches/construct-filter'
      const query = `DELETE WHERE{GRAPH?g{${toNT(ns.ex.subject)}${toNT(ns.ex.predicate)}?o}}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.removeMatches(ns.ex.subject, ns.ex.predicate)
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should handle errors', async () => {
      const id = '/remove-matches/error'

      const { store } = virtualEndpoint({ method: 'POST', id, statusCode: 500 })

      const event = store.removeMatches()

      await rejects(() => eventToPromise(event))
    })
  })

  describe('.deleteGraph', () => {
    it('should use update URL', async () => {
      const id = '/delete-graph/url'

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const stream = store.deleteGraph(ns.ex.graph)
      await eventToPromise(stream)

      strictEqual(result.touched, true)
    })

    it('should use CLEAR GRAPH query', async () => {
      const id = '/delete-graph/clear-graph'
      const query = `CLEAR GRAPH${toNT(ns.ex.graph)}`

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.deleteGraph(ns.ex.graph)
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should handle Default Graph', async () => {
      const id = '/delete-graph/clear-default-graph'
      const query = 'CLEAR GRAPH DEFAULT'

      const { result, store } = virtualEndpoint({ method: 'POST', id })

      const event = store.deleteGraph(rdf.defaultGraph())
      await eventToPromise(event)

      deepStrictEqual(result.queries, [query])
    })

    it('should throw an error if no term was given', async () => {
      const id = '/delete-graph/wrong-term-type'

      const store = new SparqlStore({ endpointUrl: `http://example.org${id}` })

      const event = store.deleteGraph()

      await rejects(() => eventToPromise(event))
    })

    it('should throw an error on wrong term type', async () => {
      const id = '/delete-graph/wrong-term-type'

      const store = new SparqlStore({ endpointUrl: `http://example.org${id}` })

      const event = store.deleteGraph(rdf.blankNode())

      await rejects(() => eventToPromise(event))
    })

    it('should handle errors', async () => {
      const id = '/delete-graph/error'

      const { store } = virtualEndpoint({ method: 'POST', id, statusCode: 500 })

      const event = store.deleteGraph(ns.ex.graph)

      await rejects(() => eventToPromise(event))
    })
  })
})
