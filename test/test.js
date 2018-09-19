/* global describe, it */

const assert = require('assert')
const nock = require('nock')
const rdf = require('@rdfjs/data-model')
const rdfExt = require('rdf-ext')
const url = require('url')
const SparqlStore = require('../')

function expectError (p) {
  return new Promise((resolve, reject) => {
    Promise.resolve().then(() => {
      return p()
    }).then(() => {
      reject(new Error('no error thrown'))
    }).catch(() => {
      resolve()
    })
  })
}

function virtualEndpoint ({ method = 'GET', id, statusCode, content, headers = {}, count = 1 } = {}) {
  const result = {
    queries: [],
    touched: false
  }

  for (let i = 0; i < count; i++) {
    let request

    if (method === 'GET') {
      request = nock('http://example.org').get(new RegExp(`${id}.*`))
    } else if (method === 'POST') {
      request = nock('http://example.org').post(id)
    }

    request.reply((uri, body) => {
      const params = (url.parse(uri).query || body || '').split('&').reduce((params, param) => {
        const pair = param.split('=')

        params[pair[0]] = decodeURIComponent(pair[1])

        return params
      }, {})

      result.touched = true

      if (params.query || params.update) {
        result.queries.push(params.query || params.update)
      }

      return [statusCode || (content ? 200 : 201), content, headers]
    })
  }

  return result
}

const example = {}

example.subject = rdf.namedNode('http://example.org/subject')
example.subjectNt = `<${example.subject.value}>`
example.predicate = rdf.namedNode('http://example.org/predicate')
example.predicateNt = `<${example.predicate.value}>`
example.object = rdf.literal('object')
example.objectNt = `"${example.object.value}"`
example.graph = rdf.namedNode('http://example.org/graph')
example.graphNt = `<${example.graph.value}>`
example.dataset = rdfExt.dataset([rdf.quad(example.subject, example.predicate, example.object, example.graph)])
example.datasetDefaultGraph = rdfExt.graph(example.dataset)

describe('store-sparql', () => {
  describe('constructor', () => {
    it('should throw an error if no endpointUrl is given', () => {
      return expectError(() => {
        const store = new SparqlStore()

        assert(!store)
      })
    })

    it('should use the given endpointUrl in the SPARQL client instance', () => {
      const endpointUrl = 'http://example.org/sparql'

      const store = new SparqlStore(endpointUrl)

      assert.strictEqual(store.client.endpointUrl, endpointUrl)
    })

    it('should use endpointUrl as updateUrl if updateUrl is not given', () => {
      const endpointUrl = 'http://example.org/sparql'

      const store = new SparqlStore(endpointUrl)

      assert.strictEqual(store.client.updateUrl, endpointUrl)
    })
  })

  describe('.construct', () => {
    it('should use the endpoint URL', () => {
      const id = '/construct/url'
      const query = `DESCRIBE${example.graphNt}`

      const result = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`, { updateUrl: `http://example.org${id}/update` })
      const stream = store.construct(query)

      return rdfExt.dataset().import(stream).then(() => {
        assert(result.touched)
      })
    })

    it('should forward the query', () => {
      const id = '/construct/send'
      const query = `DESCRIBE${example.graphNt}`

      const result = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.construct(query)

      return rdfExt.dataset().import(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should stream the result', () => {
      const id = '/construct/stream'
      const query = `DESCRIBE${example.graphNt}`

      virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.construct(query)

      return rdfExt.dataset().import(stream).then(dataset => {
        assert(example.datasetDefaultGraph.toCanonical(), dataset.toCanonical())
      })
    })

    it('should handle errors', () => {
      const id = '/construct/error'
      const query = 'not a valid SPARQL query'

      virtualEndpoint({
        id,
        statusCode: 500
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.construct(query)

      return expectError(() => rdfExt.dataset().import(stream))
    })
  })

  describe('.update', () => {
    it('should use the update URL', () => {
      const id = '/update/url'
      const query = `MOVE DEFAULT TO ${example.graphNt}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}/sparql`, { updateUrl: `http://example.org${id}` })
      const event = store.update(query)

      return rdfExt.waitFor(event).then(() => {
        assert(result.touched)
      })
    })

    it('should forward the query', () => {
      const id = '/update/send'
      const query = `MOVE DEFAULT TO ${example.graphNt}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.update(query)

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should handle errors', () => {
      const id = '/update/error'
      const query = 'not a valid SPARQL query'

      nock('http://example.org')
        .post(id)
        .reply(500)

      const store = new SparqlStore(`http://example.org/sparql${id}`)
      const event = store.update(query)

      return expectError(() => rdfExt.waitFor(event))
    })
  })

  describe('.match', () => {
    it('should use the endpoint URL', () => {
      const id = '/match/url'

      const result = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`, { updateUrl: `http://example.org${id}/update` })
      const stream = store.match()

      return rdfExt.dataset().import(stream).then(() => {
        assert(result.touched)
      })
    })

    it('should use CONSTRUCT query', () => {
      const id = '/match/construct'
      const query = 'CONSTRUCT{?s?p?o}{GRAPH?g{?s?p?o}}'

      const result = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.match()

      return rdfExt.dataset().import(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should stream the result', () => {
      const id = '/match/stream'

      virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.match()

      return rdfExt.dataset().import(stream).then(dataset => {
        assert.strictEqual(example.datasetDefaultGraph.toCanonical(), dataset.toCanonical())
      })
    })

    it('should support filters', () => {
      const id = '/match/construct-filter'
      const query = `CONSTRUCT{${example.subjectNt}${example.predicateNt}?o}{GRAPH${example.graphNt}{${example.subjectNt}${example.predicateNt}?o}}`

      const result = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.match(example.subject, example.predicate, null, example.graph)

      return rdfExt.dataset().import(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should should support Default Graph filter', () => {
      const id = '/match/construct-filter-default'
      const query = `CONSTRUCT{${example.subjectNt}${example.predicateNt}?o}{${example.subjectNt}${example.predicateNt}?o}`

      const result = virtualEndpoint({
        id,
        content: example.datasetDefaultGraph.toCanonical(),
        headers: { 'Content-Type': 'text/turtle' }
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.match(example.subject, example.predicate, null, rdf.defaultGraph())

      return rdfExt.dataset().import(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should handle errors', () => {
      const id = '/match/error'

      virtualEndpoint({
        id,
        statusCode: 500
      })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.match()

      return expectError(() => rdfExt.dataset().import(stream))
    })
  })

  describe('.import', () => {
    it('should use update URL', () => {
      const id = '/import/url'

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}/sparql`, { updateUrl: `http://example.org${id}` })
      const stream = store.import(example.dataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert(result.touched)
      })
    })

    it('should use INSERT DATA { GRAPH <...> {...} } query', () => {
      const id = '/import/insert'
      const query = `INSERT DATA{GRAPH${example.graphNt}{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.import(example.dataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should use INSERT DATA { ... } query for Default Graph', () => {
      const id = '/import/insert-default-graph'
      const query = `INSERT DATA{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.import(example.datasetDefaultGraph.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should split queries if content is to long', () => {
      const id = '/import/split-length'
      const queries = [
        `INSERT DATA{GRAPH${example.graphNt}{${example.subjectNt} ${example.predicateNt} "object1" .}}`,
        `INSERT DATA{GRAPH${example.graphNt}{${example.subjectNt} ${example.predicateNt} "object2" .}}`
      ]

      const exampleDataset = rdfExt.dataset([
        rdf.quad(example.subject, example.predicate, rdf.literal('object1'), example.graph),
        rdf.quad(example.subject, example.predicate, rdf.literal('object2'), example.graph)
      ])

      const result = virtualEndpoint({ method: 'POST', id, count: 2 })

      const store = new SparqlStore(`http://example.org${id}`, { maxQueryLength: 120 })
      const stream = store.import(exampleDataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, queries)
      })
    })

    it('should split queries on new graph', () => {
      const id = '/import/split-graph'
      const queries = [
        `INSERT DATA{GRAPH<http://example.org/graph1>{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}}`,
        `INSERT DATA{GRAPH<http://example.org/graph2>{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}}`
      ]

      const exampleDataset = rdfExt.dataset([
        rdf.quad(example.subject, example.predicate, example.object, rdf.namedNode('http://example.org/graph1')),
        rdf.quad(example.subject, example.predicate, example.object, rdf.namedNode('http://example.org/graph2'))
      ])

      const result = virtualEndpoint({ method: 'POST', id, count: 2 })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.import(exampleDataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, queries)
      })
    })

    it('should handle errors', () => {
      const id = '/import/error'

      nock('http://example.org')
        .post(id)
        .reply(500)

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.import(example.dataset.toStream())

      return expectError(() => rdfExt.waitFor(stream))
    })
  })

  describe('.remove', () => {
    it('should use update URL', () => {
      const id = '/remove/url'

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}/sparql`, { updateUrl: `http://example.org${id}` })
      const stream = store.remove(example.dataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert(result.touched)
      })
    })

    it('should use DELETE DATA {GRAPH<...>} query', () => {
      const id = '/remove/delete'
      const query = `DELETE DATA{GRAPH${example.graphNt}{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.remove(example.dataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should use DELETE DATA {} query for Default Graph', () => {
      const id = '/remove/delete-default-graph'
      const query = `DELETE DATA{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.remove(example.datasetDefaultGraph.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should split queries if content is to long', () => {
      const id = '/remove/split-length'
      const queries = [
        `DELETE DATA{GRAPH${example.graphNt}{${example.subjectNt} ${example.predicateNt} "object1" .}}`,
        `DELETE DATA{GRAPH${example.graphNt}{${example.subjectNt} ${example.predicateNt} "object2" .}}`
      ]

      const exampleDataset = rdfExt.dataset([
        rdf.quad(example.subject, example.predicate, rdf.literal('object1'), example.graph),
        rdf.quad(example.subject, example.predicate, rdf.literal('object2'), example.graph)
      ])

      const result = virtualEndpoint({ method: 'POST', id, count: 2 })

      const store = new SparqlStore(`http://example.org${id}`, { maxQueryLength: 120 })
      const stream = store.remove(exampleDataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, queries)
      })
    })

    it('should split queries on new graph', () => {
      const id = '/remove/split-graph'
      const queries = [
        `DELETE DATA{GRAPH<http://example.org/graph1>{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}}`,
        `DELETE DATA{GRAPH<http://example.org/graph2>{${example.subjectNt} ${example.predicateNt} ${example.objectNt} .}}`
      ]

      const exampleDataset = rdfExt.dataset([
        rdf.quad(example.subject, example.predicate, example.object, rdf.namedNode('http://example.org/graph1')),
        rdf.quad(example.subject, example.predicate, example.object, rdf.namedNode('http://example.org/graph2'))
      ])

      const result = virtualEndpoint({ method: 'POST', id, count: 2 })

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.remove(exampleDataset.toStream())

      return rdfExt.waitFor(stream).then(() => {
        assert.deepStrictEqual(result.queries, queries)
      })
    })

    it('should handle errors', () => {
      const id = '/remove/error'

      nock('http://example.org')
        .post(id)
        .reply(500)

      const store = new SparqlStore(`http://example.org${id}`)
      const stream = store.remove(example.dataset.toStream())

      return expectError(() => rdfExt.waitFor(stream))
    })
  })

  describe('.removeMatches', () => {
    it('should use update URL', () => {
      const id = '/remove-matches/url'

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}/sparql`, { updateUrl: `http://example.org${id}` })
      const stream = store.removeMatches()

      return rdfExt.waitFor(stream).then(() => {
        assert(result.touched)
      })
    })

    it('should use DELETE WHERE {GRAPH?g{...}} query', () => {
      const id = '/remove-matches/delete-where'
      const query = 'DELETE WHERE{GRAPH?g{?s?p?o}}'

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.removeMatches()

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should use DELETE WHERE {GRAPH<...>{...}} query for Named Graph', () => {
      const id = '/remove-matches/delete-where-graph'
      const query = `DELETE WHERE{GRAPH${example.graphNt}{${example.subjectNt}?p?o}}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.removeMatches(example.subject, null, null, example.graph)

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should use DELETE WHERE {...} query for Default Graph', () => {
      const id = '/remove-matches/delete-default-graph'
      const query = `DELETE WHERE{${example.subjectNt}?p?o}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.removeMatches(example.subject, null, null, rdf.defaultGraph())

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should use CLEAR GRAPH <...> query for graph only filter', () => {
      const id = '/remove-matches/delete-graph-only'
      const query = `CLEAR GRAPH${example.graphNt}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.removeMatches(null, null, null, example.graph)

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should support filters', () => {
      const id = '/remove-matches/construct-filter'
      const query = `DELETE WHERE{GRAPH?g{${example.subjectNt}${example.predicateNt}?o}}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.removeMatches(example.subject, example.predicate)

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should handle errors', () => {
      const id = '/remove-matches/error'

      virtualEndpoint({ method: 'POST', id, statusCode: 500 })

      const store = new SparqlStore(`http://example.org/sparql${id}`)
      const event = store.removeMatches()

      return expectError(() => rdfExt.waitFor(event))
    })
  })

  describe('.deleteGraph', () => {
    it('should use update URL', () => {
      const id = '/delete-graph/url'

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}/sparql`, { updateUrl: `http://example.org${id}` })
      const stream = store.deleteGraph(example.graph)

      return rdfExt.waitFor(stream).then(() => {
        assert(result.touched)
      })
    })

    it('should use CLEAR GRAPH query', () => {
      const id = '/delete-graph/clear-graph'
      const query = `CLEAR GRAPH${example.graphNt}`

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.deleteGraph(example.graph)

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should handle Default Graph', () => {
      const id = '/delete-graph/clear-default-graph'
      const query = 'CLEAR GRAPH DEFAULT'

      const result = virtualEndpoint({ method: 'POST', id })

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.deleteGraph(rdf.defaultGraph())

      return rdfExt.waitFor(event).then(() => {
        assert.deepStrictEqual(result.queries, [query])
      })
    })

    it('should throw an error if no term was given', () => {
      const id = '/delete-graph/wrong-term-type'

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.deleteGraph()

      return expectError(() => rdfExt.waitFor(event))
    })

    it('should throw an error on wrong term type', () => {
      const id = '/delete-graph/wrong-term-type'

      const store = new SparqlStore(`http://example.org${id}`)
      const event = store.deleteGraph(rdf.blankNode())

      return expectError(() => rdfExt.waitFor(event))
    })

    it('should handle errors', () => {
      const id = '/delete-graph/error'

      virtualEndpoint({ method: 'POST', id, statusCode: 500 })

      const store = new SparqlStore(`http://example.org/sparql${id}`)
      const event = store.deleteGraph(example.graph)

      return expectError(() => rdfExt.waitFor(event))
    })
  })
})
