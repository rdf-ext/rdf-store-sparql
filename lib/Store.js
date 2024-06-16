import rdf from 'rdf-ext'
import TripleToQuadTransform from 'rdf-transform-triple-to-quad'
import promiseToEvent from 'rdf-utils-stream/promiseToEvent.js'
import StreamClient from 'sparql-http-client/StreamClient.js'
import QuadStreamQuery from './QuadStreamQuery.js'
import QueryBuilder from './QueryBuilder.js'

class Store {
  constructor ({ endpointUrl, factory = rdf, maxQueryLength = Infinity, updateUrl } = {}) {
    if (!endpointUrl || typeof endpointUrl !== 'string') {
      throw new Error('no endpoint URL string given')
    }

    this.client = new StreamClient({
      endpointUrl,
      updateUrl: updateUrl || endpointUrl
    })
    this.factory = factory
    this.maxQueryLength = maxQueryLength
  }

  async _deleteGraph (graph) {
    const query = QueryBuilder.delete(graph)

    await this.client.query.update(query)
  }

  async _removeMatches (subject, predicate, object, graph) {
    const query = QueryBuilder.removeMatches(subject, predicate, object, graph)

    await this.client.query.update(query)
  }

  deleteGraph (graph) {
    return promiseToEvent(this._deleteGraph(graph))
  }

  import (stream) {
    return new QuadStreamQuery({
      callback: QueryBuilder.importFrame,
      client: this.client,
      factory: this.factory,
      input: stream,
      maxQueryLength: this.maxQueryLength
    })
  }

  match (subject, predicate, object, graph) {
    const query = QueryBuilder.match(subject, predicate, object, graph)

    const output = new TripleToQuadTransform(graph, { factory: this.factory })

    const stream = this.client.query.construct(query)
    stream.pipe(output)

    return output
  }

  removeMatches (subject, predicate, object, graph) {
    // use CLEAR GRAPH if only graph filter is given
    if (!subject && !predicate && !object && graph) {
      return this.deleteGraph(graph)
    }

    return promiseToEvent(this._removeMatches(subject, predicate, object, graph))
  }

  remove (stream) {
    return new QuadStreamQuery({
      callback: QueryBuilder.removeFrame,
      client: this.client,
      factory: this.factory,
      input: stream,
      maxQueryLength: this.maxQueryLength
    })
  }
}

export default Store
