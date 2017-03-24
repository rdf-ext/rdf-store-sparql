/* global describe, it */

const assert = require('assert')
const nock = require('nock')
const rdf = require('rdf-ext')
const SparqlStore = require('../')

describe('rdf-store-sparql', () => {
  const simpleDataset = rdf.dataset([
    rdf.quad(
      rdf.namedNode('http://example.org/subject'),
      rdf.namedNode('http://example.org/predicate'),
      rdf.literal('object'),
      rdf.namedNode('http://example.org/graph')
    )
  ])

  const simpleGraph = rdf.graph(simpleDataset)

  const simpleGraphNT = '<http://example.org/subject> <http://example.org/predicate> "object".'

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

  describe('constructor', () => {
    it('should throw an error if no options are given', () => {
      return expectError(() => {
        let store = new SparqlStore()

        assert(!store)
      })
    })

    it('should throw an error if no endpointUrl is given', () => {
      return expectError(() => {
        let store = new SparqlStore({})

        assert(!store)
      })
    })
  })

  describe('.construct', () => {
    const query = 'DESCRIBE<http://example.org/graph>'

    it('should forward the query and stream the result', () => {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      let graph = rdf.graph()
      let stream = store.construct(query)

      return graph.import(stream).then(() => {
        assert(simpleGraph.equals(graph))
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(500)

      let store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      return expectError(() => {
        return rdf.dataset().import(store.construct(query))
      })
    })
  })

  describe('.update', () => {
    const query = 'MOVE DEFAULT TO <http://example.org/graph>'

    it('should forward the query', () => {
      let sentData

      nock('http://example.org')
        .post('/update')
        .reply(204, (url, body) => {
          sentData = decodeURIComponent(body.slice(7))
        })

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return rdf.waitFor(store.update(query)).then(() => {
        assert.equal(sentData, query)
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return expectError(() => {
        return rdf.waitFor(store.update(query))
      })
    })
  })

  describe('.match', () => {
    const query = 'CONSTRUCT{?s?p?o}{GRAPH?g{?s?p?o}}'

    it('should use CONSTRUCT query and stream the result', () => {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      let graph = rdf.graph()
      let stream = store.match()

      return graph.import(stream).then(() => {
        assert(simpleGraph.equals(graph))
      })
    })

    it('should use CONSTRUCT query with filter and stream the result', () => {
      const query = 'CONSTRUCT{<http://example.org/subject><http://example.org/predicate>?o}{GRAPH<http://example.org/graph>{<http://example.org/subject><http://example.org/predicate>?o}}'

      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      let stream = store.match(
        rdf.namedNode('http://example.org/subject'),
        rdf.namedNode('http://example.org/predicate'),
        null,
        rdf.namedNode('http://example.org/graph')
      )

      return rdf.dataset().import(stream).then((dataset) => {
        assert(simpleDataset.equals(dataset))
      })
    })

    it('should use CONSTRUCT query with filter in default graph and stream the result', () => {
      const query = 'CONSTRUCT{<http://example.org/subject><http://example.org/predicate>?o}{{<http://example.org/subject><http://example.org/predicate>?o}}'

      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      let stream = store.match(
        rdf.namedNode('http://example.org/subject'),
        rdf.namedNode('http://example.org/predicate'),
        null,
        rdf.defaultGraph()
      )

      return rdf.dataset().import(stream).then((dataset) => {
        assert(simpleGraph.equals(dataset))
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(500)

      let store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      return expectError(() => {
        return rdf.dataset().import(store.match())
      })
    })
  })

  describe('.import', () => {
    const query = 'INSERT DATA{GRAPH<http://example.org/graph>{<http://example.org/subject> <http://example.org/predicate> "object" .\n}}'

    it('should use INSERT DATA query', () => {
      let sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, (url, body) => {
          sentData = decodeURIComponent(body.slice(7))
        })

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return rdf.waitFor(store.import(simpleDataset.toStream())).then(() => {
        assert.equal(sentData, query)
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return expectError(() => {
        return rdf.waitFor(store.import(simpleDataset.toStream()))
      })
    })
  })

  describe('.import with truncate', () => {
    const query = 'DROP SILENT GRAPH<http://example.org/graph>;INSERT DATA{GRAPH<http://example.org/graph>{<http://example.org/subject> <http://example.org/predicate> "object" .\n}}'

    it('should use DROP SILENT and INSERT DATA query', () => {
      let sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, (url, body) => {
          sentData = decodeURIComponent(body.slice(7))
        })

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return rdf.waitFor(store.import(simpleDataset.toStream(), {truncate: true})).then(() => {
        assert.equal(sentData, query)
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return expectError(() => {
        return rdf.waitFor(store.import(simpleDataset.toStream(), {truncate: true}))
      })
    })
  })

  describe('.remove', () => {
    const query = 'DELETE DATA FROM<http://example.org/graph>{<http://example.org/subject> <http://example.org/predicate> "object" .\n}'

    it('should use DELETE DATA FROM query', () => {
      let sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, (url, body) => {
          sentData = decodeURIComponent(body.slice(7))
        })

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return rdf.waitFor(store.remove(simpleDataset.toStream())).then(() => {
        assert.equal(sentData, query)
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return expectError(() => {
        return rdf.waitFor(store.remove(simpleDataset.toStream()))
      })
    })
  })

  describe('.removeMatches', () => {
    const query = 'DELETE FROM GRAPH<http://example.org/graph>WHERE{<http://example.org/subject><http://example.org/predicate>?o}'

    it('should use DELETE FROM GRAPH query', () => {
      let sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, (url, body) => {
          sentData = decodeURIComponent(body.slice(7))
        })

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return rdf.waitFor(store.removeMatches(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          null,
          rdf.namedNode('http://example.org/graph')
      )).then(() => {
        assert.equal(sentData, query)
      })
    })

    it('should handle errors', () => {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return expectError(() => {
        return rdf.waitFor(store.removeMatches(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          null,
          rdf.namedNode('http://example.org/graph')
        ))
      })
    })
  })

  describe('.deleteGraph', () => {
    const query = 'CLEAR GRAPH<http://example.org/graph>'

    it('should use CLEAR GRAPH query', () => {
      let sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, (url, body) => {
          sentData = decodeURIComponent(body.slice(7))
        })

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return rdf.waitFor(store.deleteGraph(rdf.namedNode('http://example.org/graph'))).then(() => {
        assert.equal(sentData, query)
      })
    })

    it('should handle error with callback interface', () => {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      let store = new SparqlStore({
        endpointUrl: 'http://example.org/sparql',
        updateUrl: 'http://example.org/update'
      })

      return expectError(() => {
        return rdf.waitFor(store.deleteGraph(rdf.namedNode('http://example.org/graph')))
      })
    })
  })
})
