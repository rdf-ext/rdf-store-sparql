var inherits = require('inherits')
var N3Parser = require('rdf-parser-n3')
var NTriplesSerializer = require('rdf-serializer-ntriples')
var AbstractStore = require('rdf-store-abstract')
var SparqlHttpClient = require('sparql-http-client')

function buildMatch (subject, predicate, object) {
  var match = ''

  var nodeToNT = function (node) {
    if (typeof node === 'string') {
      if (node.substr(0, 2) === '_:') {
        return node
      } else {
        return '<' + node + '>'
      }
    }

    return node.toNT()
  }

  match += subject ? nodeToNT(subject) : '?s'
  match += predicate ? nodeToNT(predicate) : '?p'
  match += object ? nodeToNT(object) : '?o'

  return match
}

function checkStatusCode (result) {
  if (result.statusCode < 200 || result.statusCode >= 300) {
    var error = new Error('status code: ' + result.statusCode)

    error.statusCode = result.statusCode

    throw error
  }
}

function combinedCallback (resolve, reject, callback) {
  callback = callback || function () {}

  return function (error, result) {
    if (!error) {
      callback(null, result)
      resolve(result)
    } else {
      callback(error)
      reject(error)
    }
  }
}

function SparqlStore (options) {
  if (!options || !options.endpointUrl) {
    throw new Error('SparqlStore requires at least an endpointUrl')
  }

  this.rdf = options.rdf || require('rdf-ext')
  this.serialize = options.serialize || NTriplesSerializer.serialize.bind(NTriplesSerializer)
  this.parse = options.parse || N3Parser.parse.bind(N3Parser)

  var defaultRequest = this.rdf.defaultRequest

  this.client = new SparqlHttpClient({
    endpointUrl: options.endpointUrl,
    updateUrl: options.updateUrl || options.endpointUrl,
    fetch: options.request || function (url, options) {
      options.url = url
      return defaultRequest(options)
    }
  })

  this.client.types.construct.accept = options.mimeType || 'application/n-triples'

  AbstractStore.call(this)
}

inherits(SparqlStore, AbstractStore)

SparqlStore.prototype.add = function (graphIri, graph, callback) {
  var self = this

  return new Promise(function (resolve, reject) {
    callback = combinedCallback(resolve, reject, callback)

    self.serialize(graph).then(function (serialized) {
      var query = 'DROP SILENT GRAPH<' + graphIri + '>;INSERT DATA{GRAPH<' + graphIri + '>{' + serialized + '}}'

      return self.client.updateQuery(query)
    }).then(function (result) {
      checkStatusCode(result)

      callback(null, graph)
    }).catch(function (error) {
      callback(error)
    })
  })
}

SparqlStore.prototype.delete = function (graphIri, callback) {
  var self = this

  return new Promise(function (resolve, reject) {
    callback = combinedCallback(resolve, reject, callback)

    var query = 'CLEAR GRAPH<' + graphIri + '>'

    self.client.updateQuery(query).then(function (result) {
      checkStatusCode(result)

      callback()
    }).catch(function (error) {
      callback(error)
    })
  })
}

SparqlStore.prototype.graph = function (iri, callback) {
  return this.match(null, null, null, iri, callback)
}

SparqlStore.prototype.match = function (subject, predicate, object, iri, callback, limit) {
  var self = this

  return new Promise(function (resolve, reject) {
    callback = combinedCallback(resolve, reject, callback)

    var filter = buildMatch(subject, predicate, object)
    var query = 'CONSTRUCT{' + filter + '}{GRAPH<' + iri + '>{' + filter + '}}'

    self.client.constructQuery(query).then(function (result) {
      checkStatusCode(result)

      return self.parse(result.content)
    }).then(function (graph) {
      callback(null, graph)
    }).catch(function (error) {
      callback(error)
    })
  })
}

SparqlStore.prototype.merge = function (graphIri, graph, callback) {
  var self = this

  return new Promise(function (resolve, reject) {
    callback = combinedCallback(resolve, reject, callback)

    self.serialize(graph).then(function (serialized) {
      var query = 'INSERT DATA{GRAPH<' + graphIri + '>{' + serialized + '}}'

      return self.client.updateQuery(query)
    }).then(function (result) {
      checkStatusCode(result)

      callback(null, graph)
    }).catch(function (error) {
      callback(error)
    })
  })
}

SparqlStore.prototype.remove = function (graphIri, graph, callback) {
  var self = this

  return new Promise(function (resolve, reject) {
    callback = combinedCallback(resolve, reject, callback)

    self.serialize(graph).then(function (serialized) {
      var query = 'DELETE DATA FROM<' + graphIri + '>{' + serialized + '}'

      return self.client.updateQuery(query)
    }).then(function (result) {
      checkStatusCode(result)

      callback()
    }).catch(function (error) {
      callback(error)
    })
  })
}

SparqlStore.prototype.removeMatches = function (subject, predicate, object, iri, callback) {
  var self = this

  return new Promise(function (resolve, reject) {
    callback = combinedCallback(resolve, reject, callback)

    var query = 'DELETE FROM GRAPH<' + iri + '>WHERE{' + buildMatch(subject, predicate, object) + '}'

    self.client.updateQuery(query).then(function (result) {
      checkStatusCode(result)

      callback()
    }).catch(function (error) {
      callback(error)
    })
  })
}

module.exports = SparqlStore
