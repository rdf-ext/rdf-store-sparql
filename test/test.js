/* global describe, it */
var assert = require('assert')
var nock = require('nock')
var rdf = require('rdf-ext')
var url = require('url')
var SparqlStore = require('../').bind(null, rdf)

var createParse = function (buildGraphData) {
  return function (content, callback, base) {
    var graphData = null

    if (buildGraphData) {
      graphData = buildGraphData({
        content: content,
        base: base
      })
    }

    graphData = graphData || {graph: rdf.createGraph()}

    callback(graphData.error, graphData.graph)
  }
}

var createSerialize = function (buildSerializedData) {
  return function (graph, callback) {
    var serializedData = null

    if (buildSerializedData != null) {
      serializedData = buildSerializedData({
        graph: graph
      })
    }

    serializedData = serializedData || {content: ''}

    callback(serializedData.error, serializedData.content)
  }
}

describe('SPARQL Store', function () {
  describe('executeQuery', function () {
    it('should use http GET method on endpoint with encoded query parameter', function (done) {
      var processed = false

      nock('http://localhost')
        .get('/sparql?query=SELECT%20*%20%7B%3Fs%20%3Fp%20%3Fo%7D')
        .reply(200, function (iri, body) {
          processed = true
        })

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert(!error)
        assert(processed)

        done()
      })
    })

    it('should parse response', function (done) {
      nock('http://localhost')
        .get('/sparql?query=SELECT%20*%20%7B%3Fs%20%3Fp%20%3Fo%7D')
        .reply(200, 'test')

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse(function (params) {
          return {graph: params.content + '1'}
        })
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error, result) {
        assert(!error)
        assert.equal(result, 'test1')

        done()
      })
    })

    it('should forward request error', function (done) {
      nock('http://localhost')
        .get('/sparql?query=SELECT%20*%20%7B%3Fs%20%3Fp%20%3Fo%7D')
        .socketDelay(2000)
        .reply(200, function () {
          this.req.abort()
        })

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert(!!error)

        done()
      })
    })

    it('should forward http error status codes', function (done) {
      nock('http://localhost')
        .get('/sparql?query=SELECT%20*%20%7B%3Fs%20%3Fp%20%3Fo%7D')
        .reply(500)

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert(!!error)

        done()
      })
    })

    it('should forward parser error', function (done) {
      nock('http://localhost')
        .get('/sparql?query=SELECT%20*%20%7B%3Fs%20%3Fp%20%3Fo%7D')
        .reply(200, 'test')

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse(function () {
          return {error: 'test'}
        })
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert(!!error)

        done()
      })
    })
  })

  describe('executeUpdateQuery', function () {
    it('should use http POST method on endpoint with content', function (done) {
      var sentData

      nock('http://localhost')
        .post('/update')
        .reply(204, function (iri, body) {
          sentData = body
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error, result) {
        assert(!error)
        assert.equal(sentData, 'INSERT DATA { <http://example/subject> <http://example/predicate> "".}')

        done()
      })
    })

    it('should parse response', function (done) {
      nock('http://localhost')
        .post('/update')
        .reply(200, 'test')

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse(function (params) {
          return {graph: params.content + '1'}
        })
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error, graph) {
        assert(!error)
        assert.equal(graph, 'test1')

        done()
      })
    })

    it('should forward request error', function (done) {
      nock('http://localhost')
        .post('/update')
        .socketDelay(2000)
        .reply(204, function () {
          this.req.abort()
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error) {
        assert(!!error)

        done()
      })
    })

    it('should forward http error status codes', function (done) {
      nock('http://localhost')
        .post('/update')
        .reply(500)

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error) {
        assert(!!error)

        done()
      })
    })

    it('should forward parser error', function (done) {
      nock('http://localhost')
        .post('/update')
        .reply(200, '')

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse(function () {
          return {error: 'test'}
        })
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error) {
        assert(!!error)

        done()
      })
    })
  })

  describe('graph', function () {
    it('should use CONSTRUCT query', function (done) {
      nock('http://localhost')
        .get('/sparql?query=CONSTRUCT%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D%20%7B%20GRAPH%20%3Chttp%3A%2F%2Fexample.org%2Fgraph%3E%20%7B%3Fs%20%3Fp%20%3Fo%20%7D%7D')
        .reply(200)

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.graph('http://example.org/graph', function (error) {
        assert(!error)

        done()
      })
    })
  })

  describe('match', function () {
    it('should use CONSTRUCT query', function (done) {
      nock('http://localhost')
        .get('/sparql?query=CONSTRUCT%20%7B%20%3Chttp%3A%2F%2Fexample.org%2Fsubject%3E%20%3Fp%20%3Fo%20%7D%20%7B%20GRAPH%20%3Chttp%3A%2F%2Fexample.org%2Fgraph%3E%20%7B%3Chttp%3A%2F%2Fexample.org%2Fsubject%3E%20%3Fp%20%3Fo%20%7D%7D')
        .reply(200)

      var options = {
        endpointUrl: 'http://localhost/sparql',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.match('http://example.org/graph', 'http://example.org/subject', null, null, function (error) {
        assert(!error)

        done()
      })
    })
  })

  describe('add', function () {
    it('should use DROP SILENT GRAPH and INSERT DATA query', function (done) {
      var sentData

      nock('http://localhost')
        .post('/update')
        .reply(200, function (iri, body) {
          sentData = body
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse(),
        serialize: createSerialize()
      }

      var store = new SparqlStore(options)

      store.add('http://example.org/graph', 'test', function (error, graph) {
        assert(!error)
        assert.equal(graph, 'test')
        assert.notEqual(sentData.indexOf('DROP SILENT GRAPH'), -1)
        assert.notEqual(sentData.indexOf('INSERT DATA'), -1)

        done()
      })
    })
  })

  describe('merge', function () {
    it('should use INSERT DATA query', function (done) {
      var sentData

      nock('http://localhost')
        .post('/update')
        .reply(200, function (iri, body) {
          sentData = body
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse(),
        serialize: createSerialize()
      }

      var store = new SparqlStore(options)

      store.merge('http://example.org/graph', 'test', function (error, graph) {
        assert(!error)
        assert.equal(graph, 'test')

        done()
      })
    })
  })

  describe('remove', function () {
    it('should use DELETE DATA FROM query', function (done) {
      var sentData

      nock('http://localhost')
        .post('/update')
        .reply(200, function (iri, body) {
          sentData = body
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse(),
        serialize: createSerialize()
      }

      var store = new SparqlStore(options)

      store.remove('http://example.org/graph', null, function (error) {
        assert(!error)
        assert.notEqual(sentData.indexOf('DELETE DATA FROM'), -1)

        done()
      })
    })
  })

  describe('removeMatches', function () {
    it('should use DELETE FROM GRAPH query', function (done) {
      var sentData

      nock('http://localhost')
        .post('/update')
        .reply(200, function (iri, body) {
          sentData = body
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.removeMatches('http://example.org/graph', 'http://example.org/subject', null, null, function (error) {
        assert(!error)
        assert.notEqual(sentData.indexOf('DELETE FROM GRAPH'), -1)

        done()
      })
    })
  })

  describe('delete', function () {
    it('should use CLEAR GRAPH query', function (done) {
      var sentData

      nock('http://localhost')
        .post('/update')
        .reply(200, function (iri, body) {
          sentData = body
        })

      var options = {
        updateUrl: 'http://localhost/update',
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.delete('http://example.org/graph', function (error) {
        assert(!error)
        assert.notEqual(sentData.indexOf('CLEAR GRAPH'), -1)

        done()
      })
    })
  })
})
