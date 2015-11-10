/* global describe, it */
var assert = require('assert')
var nock = require('nock')
var rdf = require('rdf-ext')
var SparqlStore = require('../')

describe('rdf-store-sparql', function () {
  var simpleGraph = rdf.createGraph([
    rdf.createTriple(
      rdf.createNamedNode('http://example.org/subject'),
      rdf.createNamedNode('http://example.org/predicate'),
      rdf.createLiteral('object'))
  ])
  var simpleGraphNT = '<http://example.org/subject> <http://example.org/predicate> "object".'

  describe('constructor', function () {
    it('should throw an error if no options are given', function (done) {
      Promise.resolve().then(function () {
        var store = new SparqlStore()

        assert(store)

        done('no error thrown')
      }).catch(function () {
        done()
      })
    })

    it('should throw an error if no endpointUrl is given', function (done) {
      Promise.resolve().then(function () {
        var store = new SparqlStore({})

        assert(store)

        done('no error thrown')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.add', function () {
    var query = 'DROP SILENT GRAPH<http://example.org/graph>;INSERT DATA{GRAPH<http://example.org/graph>{<http://example.org/subject> <http://example.org/predicate> "object" .\n}}'

    it('should use DROP SILENT and INSERT DATA query and support callback interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.add('http://example.org/graph', simpleGraph, function (error, graph) {
        Promise.resolve().then(function () {
          assert(!error)
          assert.equal(sentData, query)
          assert(simpleGraph.equals(graph))

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.add('http://example.org/graph', simpleGraph, function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use DROP SILENT and INSERT DATA query and support Promise interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.add('http://example.org/graph', simpleGraph).then(function (graph) {
        assert.equal(sentData, query)
        assert(simpleGraph.equals(graph))

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.add('http://example.org/graph', simpleGraph).then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.delete', function () {
    var query = 'CLEAR GRAPH<http://example.org/graph>'

    it('should use CLEAR GRAPH query and support callback interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.delete('http://example.org/graph', function (error) {
        Promise.resolve().then(function () {
          assert(!error)
          assert.equal(sentData, query)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.delete('http://example.org/graph', function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use CLEAR GRAPH query and support Promise interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.delete('http://example.org/graph').then(function () {
        assert.equal(sentData, query)

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.delete('http://example.org/graph').then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.graph', function () {
    var query = 'CONSTRUCT{?s?p?o}{GRAPH<http://example.org/graph>{?s?p?o}}'

    it('should use CONSTRUCT query and support callback interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'application/n-triples'})

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.graph('http://example.org/graph', function (error, graph) {
        Promise.resolve().then(function () {
          assert(!error)
          assert(simpleGraph.equals(graph))

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.graph('http://example.org/graph', function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use CONSTRUCT query and support Promise interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'application/n-triples'})

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.graph('http://example.org/graph').then(function (graph) {
        assert(simpleGraph.equals(graph))

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.graph('http://example.org/graph').then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.match', function () {
    var query = 'CONSTRUCT{<http://example.org/subject><http://example.org/predicate>?o}{GRAPH<http://example.org/graph>{<http://example.org/subject><http://example.org/predicate>?o}}'

    it('should use CONSTRUCT query and support callback interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'application/n-triples'})

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.match('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph', function (error, graph) {
        Promise.resolve().then(function () {
          assert(!error)
          assert(simpleGraph.equals(graph))

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.match('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph', function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use CONSTRUCT query and support Promise interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(200, simpleGraphNT, {'Content-Type': 'application/n-triples'})

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.match('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph').then(function (graph) {
        assert(simpleGraph.equals(graph))

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .get('/sparql?query=' + encodeURIComponent(query))
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql'})

      store.match('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph').then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.merge', function () {
    var query = 'INSERT DATA{GRAPH<http://example.org/graph>{<http://example.org/subject> <http://example.org/predicate> "object" .\n}}'

    it('should use INSERT DATA query and support callback interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.merge('http://example.org/graph', simpleGraph, function (error, graph) {
        Promise.resolve().then(function () {
          assert(!error)
          assert.equal(sentData, query)
          assert(simpleGraph.equals(graph))

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.merge('http://example.org/graph', simpleGraph, function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use INSERT DATA query and support Promise interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.merge('http://example.org/graph', simpleGraph).then(function (graph) {
        assert.equal(sentData, query)
        assert(simpleGraph.equals(graph))

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.merge('http://example.org/graph', simpleGraph).then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.remove', function () {
    var query = 'DELETE DATA FROM<http://example.org/graph>{<http://example.org/subject> <http://example.org/predicate> "object" .\n}'

    it('should use DELETE DATA FROM query and support callback interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.remove('http://example.org/graph', simpleGraph, function (error) {
        Promise.resolve().then(function () {
          assert(!error)
          assert.equal(sentData, query)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.remove('http://example.org/graph', simpleGraph, function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use DELETE DATA FROM query and support Promise interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.remove('http://example.org/graph', simpleGraph).then(function () {
        assert.equal(sentData, query)

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.remove('http://example.org/graph', simpleGraph).then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })

  describe('.removeMatches', function () {
    var query = 'DELETE FROM GRAPH<http://example.org/graph>WHERE{<http://example.org/subject><http://example.org/predicate>?o}'

    it('should use DELETE FROM GRAPH query and support callback interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.removeMatches('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph', function (error) {
        Promise.resolve().then(function () {
          assert(!error)
          assert.equal(sentData, query)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should handle error with callback interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.removeMatches('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph', function (error) {
        Promise.resolve().then(function () {
          assert(error)

          done()
        }).catch(function (error) {
          done(error)
        })
      })
    })

    it('should use DELETE FROM GRAPH query and support Promise interface', function (done) {
      var sentData

      nock('http://example.org')
        .post('/update')
        .reply(201, function (url, body) {
          sentData = decodeURIComponent(body.slice(6))
        })

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.removeMatches('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph').then(function () {
        assert.equal(sentData, query)

        done()
      }).catch(function (error) {
        done(error)
      })
    })

    it('should handle error with Promise interface', function (done) {
      nock('http://example.org')
        .post('/update')
        .reply(500)

      var store = new SparqlStore({endpointUrl: 'http://example.org/sparql', updateUrl: 'http://example.org/update'})

      store.removeMatches('http://example.org/subject', 'http://example.org/predicate', null, 'http://example.org/graph').then(function () {
        done('no error throw')
      }).catch(function () {
        done()
      })
    })
  })
})
