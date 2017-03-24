const rdf = require('rdf-ext')
const rdfFetch = require('rdf-fetch-lite')
const N3Parser = require('rdf-parser-n3')

const formats = {
  parsers: new rdf.Parsers({
    'text/turtle': new N3Parser()
  })
}

function fetch (factory, url, options) {
  options = options || {}
  options.factory = factory
  options.formats = formats
  options.headers = options.headers || {}
  options.headers.accept = 'text/turtle'

  return rdfFetch(url, options)
}

module.exports = fetch
