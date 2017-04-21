const fetch = require('./fetch')
const rdf = require('rdf-ext')
const TripleToQuadTransform = require('rdf-transform-triple-to-quad')
const SparqlHttpClient = require('sparql-http-client')

class Store {
  constructor (options) {
    if (!options) {
      throw new Error('options not given')
    }

    if (!options.endpointUrl) {
      throw new Error('endpointUrl not given')
    }

    this.factory = options.factory || rdf

    this.client = new SparqlHttpClient({
      endpointUrl: options.endpointUrl,
      updateUrl: options.updateUrl || options.endpointUrl,
      fetch: fetch.bind(null, this.factory)
    })
  }

  construct (query, graph) {
    let stream = new TripleToQuadTransform(graph, {factory: this.factory})

    this.client.constructQuery(query).then(Store.checkStatusCode).then((res) => {
      return res.quadStream()
    }).then((quadStream) => {
      quadStream.pipe(stream)
    }).catch((err) => {
      stream.emit('error', err)
    })

    return stream
  }

  update (query) {
    return rdf.asEvent(() => {
      return this.client.updateQuery(query).then(Store.checkStatusCode)
    })
  }

  match (subject, predicate, object, graph) {
    const filter = Store.buildTripleMatch(subject, predicate, object)

    // search in all graphs by default
    let graphFilter = 'GRAPH?g'

    // search in named graph if NamedNode is given
    if (graph && graph.termType === 'NamedNode') {
      graphFilter = 'GRAPH<' + graph.value + '>'
    }

    // search in default graph if DefaultGraph is given
    if (graph && graph.termType === 'DefaultGraph') {
      graphFilter = ''
    }

    const query = 'CONSTRUCT{' + filter + '}{' + graphFilter + '{' + filter + '}}'

    return this.construct(query, graph)
  }

  import (stream, options) {
    options = options || {}

    return rdf.asEvent(() => {
      return rdf.dataset().import(stream).then((dataset) => {
        const graphIri = dataset.toArray().shift().graph.value
        const ntriples = rdf.graph(dataset).toCanonical()
        const truncateQuery = options.truncate ? 'DROP SILENT GRAPH<' + graphIri + '>;' : ''
        const query = truncateQuery + 'INSERT DATA{GRAPH<' + graphIri + '>{' + ntriples + '}}'

        return this.client.updateQuery(query)
      }).then(Store.checkStatusCode)
    })
  }

  remove (stream) {
    return rdf.asEvent(() => {
      return rdf.dataset().import(stream).then((dataset) => {
        const graphIri = dataset.toArray().shift().graph.value
        const ntriples = rdf.graph(dataset).toCanonical()
        const query = 'DELETE DATA FROM<' + graphIri + '>{' + ntriples + '}'

        return this.client.updateQuery(query)
      }).then(Store.checkStatusCode)
    })
  }

  removeMatches (subject, predicate, object, graph) {
    return rdf.asEvent(() => {
      const filter = Store.buildTripleMatch(subject, predicate, object)
      const graphFilter = Store.buildTermMatch(graph) || '?g'
      const query = 'DELETE FROM GRAPH' + graphFilter + 'WHERE{' + filter + '}'

      return this.client.updateQuery(query).then(Store.checkStatusCode)
    })
  }

  deleteGraph (graph) {
    return rdf.asEvent(() => {
      const query = 'CLEAR GRAPH<' + graph.value + '>'

      return this.client.updateQuery(query).then(Store.checkStatusCode)
    })
  }

  static buildTermMatch (term) {
    if (!term) {
      return null
    }

    if (term.termType !== 'NamedNode' && term.termType !== 'Literal') {
      return null
    }

    return term.toCanonical()
  }

  static buildTripleMatch (subject, predicate, object) {
    return [
      Store.buildTermMatch(subject) || '?s',
      Store.buildTermMatch(predicate) || '?p',
      Store.buildTermMatch(object) || '?o'
    ].join('')
  }

  static checkStatusCode (res) {
    if (res.status >= 400) {
      let err = new Error('http error: ' + res.statusText)
      err.status = res.status

      return Promise.reject(err)
    }

    return res
  }
}

module.exports = Store
