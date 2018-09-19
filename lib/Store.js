const asEvent = require('./asEvent')
const checkStatusCode = require('./checkStatusCode')
const fetch = require('./fetch')
const rdf = require('@rdfjs/data-model')
const QuadStreamQuery = require('./QuadStreamQuery')
const QueryBuilder = require('./QueryBuilder')
const TripleToQuadTransform = require('rdf-transform-triple-to-quad')
const SparqlHttpClient = require('sparql-http-client')

class Store {
  constructor (endpointUrl, { updateUrl, factory = rdf, maxQueryLength = Infinity } = {}) {
    if (!endpointUrl || typeof endpointUrl !== 'string') {
      throw new Error('no endpoint URL string given')
    }

    this.factory = factory
    this.maxQueryLength = maxQueryLength

    this.client = new SparqlHttpClient({
      endpointUrl: endpointUrl,
      updateUrl: updateUrl || endpointUrl,
      fetch: fetch.bind(null, this.factory)
    })
  }

  construct (query, graph) {
    const toQuad = new TripleToQuadTransform(graph, { factory: this.factory })

    this.client.constructQuery(query).then(checkStatusCode).then((res) => {
      return res.quadStream()
    }).then(quadStream => {
      quadStream.pipe(toQuad)
    }).catch((err) => {
      toQuad.emit('error', err)
    })

    return toQuad
  }

  update (query) {
    return asEvent(() => {
      return this.client.updateQuery(query).then(checkStatusCode)
    })
  }

  match (subject, predicate, object, graph) {
    return this.construct(QueryBuilder.match(subject, predicate, object, graph), graph)
  }

  import (stream) {
    return new QuadStreamQuery(this.client, stream, this.maxQueryLength, QueryBuilder.importFrame)
  }

  remove (stream) {
    return new QuadStreamQuery(this.client, stream, this.maxQueryLength, QueryBuilder.removeFrame)
  }

  removeMatches (subject, predicate, object, graph) {
    // use CLEAR GRAPH if only graph filter is given
    if (!subject && !predicate && !object && graph) {
      return this.deleteGraph(graph)
    }

    return asEvent(() => {
      return this.client.updateQuery(QueryBuilder.removeMatches(subject, predicate, object, graph)).then(checkStatusCode)
    })
  }

  deleteGraph (graph) {
    return asEvent(() => {
      return this.client.updateQuery(QueryBuilder.delete(graph)).then(checkStatusCode)
    })
  }
}

module.exports = Store
