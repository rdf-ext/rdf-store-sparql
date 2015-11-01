/* global describe, it */
var assert = require('assert')
var rdf = require('rdf-ext')
var url = require('url')
var SparqlStore = require('../').bind(null, rdf)

// TODO: add to test utils
var createClient = function (buildResponse) {
  return function (method, url, headers, content, callback) {
    var res = null

    if (buildResponse != null) {
      res = buildResponse({
        method: method,
        url: url,
        headers: headers,
        content: content
      })
    }

    res = res || {}
    res.statusCode = res.statusCode !== undefined ? res.statusCode : 200
    res.headers = res.headers || {}
    res.content = res.content || ''

    callback(res.statusCode, res.headers, res.content, res.error)
  }
}

var createParse = function (buildGraphData) {
  return function (content, callback, base) {
    var graphData = null

    if (buildGraphData != null) {
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
    it('should use http GET method', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          assert.equal(req.method, 'GET')
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function () {
        done()
      })
    })

    it('should use endpointUrl option', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          var parsed = url.parse(req.url)

          assert.equal(parsed.pathname, 'http//localhost/sparql')
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function () {
        done()
      })
    })

    it('should forward query as encoded parameter', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          var parsed = url.parse(req.url)

          assert.equal(parsed.query, 'query=SELECT%20*%20%7B%3Fs%20%3Fp%20%3Fo%7D')
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function () {
        done()
      })
    })

    it('should parse response', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          return {content: 'test'}
        }),
        parse: createParse(function (params) {
          return {graph: params.content + '1'}
        })
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error, graph) {
        assert.equal(error, null)
        assert.equal(graph, 'test1')

        done()
      })
    })

    it('should forward request error', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          return {error: 'test'}
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert.notEqual(error, null)

        done()
      })
    })

    it('should forward http error status codes', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function () {
          return {statusCode: 500}
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert.notEqual(error, null)

        done()
      })
    })

    it('should forward parser error', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(),
        parse: createParse(function () {
          return {error: 'test'}
        })
      }

      var store = new SparqlStore(options)

      store.executeQuery('SELECT * {?s ?p ?o}', function (error) {
        assert.notEqual(error, null)

        done()
      })
    })
  })

  describe('executeUpdateQuery', function () {
    it('should use http POST method', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.method, 'POST')
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function () {
        done()
      })
    })

    it('should use endpointUrl option', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          var parsed = url.parse(req.url)

          assert.equal(parsed.pathname, 'http//localhost/update')
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function () {
        done()
      })
    })

    it('should forward query as content', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.content, 'INSERT DATA { <http://example/subject> <http://example/predicate> "".}')
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function () {
        done()
      })
    })

    it('should parse response', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          return {content: 'test'}
        }),
        parse: createParse(function (params) {
          return {graph: params.content + '1'}
        })
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error, graph) {
        assert.equal(error, null)
        assert.equal(graph, 'test1')

        done()
      })
    })

    it('should forward request error', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          return {error: 'test'}
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error) {
        assert.notEqual(error, null)

        done()
      })
    })

    it('should forward http error status codes', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function () {
          return {statusCode: 500}
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error) {
        assert.notEqual(error, null)

        done()
      })
    })

    it('should forward parser error', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(),
        parse: createParse(function () {
          return {error: 'test'}
        })
      }

      var store = new SparqlStore(options)

      store.executeUpdateQuery('INSERT DATA { <http://example/subject> <http://example/predicate> "".}', function (error) {
        assert.notEqual(error, null)

        done()
      })
    })
  })

  describe('graph', function () {
    it('should use CONSTRUCT query', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          var parsed = url.parse(req.url)

          assert.equal(parsed.query.indexOf('query=CONSTRUCT'), 0)
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.graph('http://example.org/graph', function () {
        done()
      })
    })
  })

  describe('match', function () {
    it('should use CONSTRUCT query', function (done) {
      var options = {
        endpointUrl: 'http//localhost/sparql',
        request: createClient(function (req) {
          var parsed = url.parse(req.url)

          assert.equal(parsed.query.indexOf('query=CONSTRUCT'), 0)
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.match('http://example.org/graph', 'http://example.org/subject', null, null, function () {
        done()
      })
    })
  })

  describe('add', function () {
    it('should use DROP SILENT GRAPH and INSERT DATA query', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.content.indexOf('DROP SILENT GRAPH'), 0)
          assert.equal(req.content.indexOf('INSERT DATA') > 0, true)
        }),
        parse: createParse(),
        serialize: createSerialize()
      }

      var store = new SparqlStore(options)

      store.add('http://example.org/graph', null, function () {
        done()
      })
    })
  })

  describe('merge', function () {
    it('should use INSERT DATA query', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.content.indexOf('INSERT DATA'), 0)
        }),
        parse: createParse(),
        serialize: createSerialize()
      }

      var store = new SparqlStore(options)

      store.merge('http://example.org/graph', null, function () {
        done()
      })
    })
  })

  describe('remove', function () {
    it('should use DELETE DATA FROM query', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.content.indexOf('DELETE DATA FROM'), 0)
        }),
        parse: createParse(),
        serialize: createSerialize()
      }

      var store = new SparqlStore(options)

      store.remove('http://example.org/graph', null, function () {
        done()
      })
    })
  })

  describe('removeMatches', function () {
    it('should use DELETE FROM GRAPH query', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.content.indexOf('DELETE FROM GRAPH'), 0)
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.removeMatches('http://example.org/graph', 'http://example.org/subject', null, null, function () {
        done()
      })
    })
  })

  describe('delete', function () {
    it('should use CLEAR  GRAPH query', function (done) {
      var options = {
        updateUrl: 'http//localhost/update',
        request: createClient(function (req) {
          assert.equal(req.content.indexOf('CLEAR  GRAPH'), 0)
        }),
        parse: createParse()
      }

      var store = new SparqlStore(options)

      store.delete('http://example.org/graph', function () {
        done()
      })
    })
  })
})
