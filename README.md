# rdf-store-sparql

SPARQL RDF Store that follows the [RDF/JS Store interface](http://rdf.js.org/stream-spec/#store-interface) specification.
This store implementation allows accessing graphs using the [SPARQL 1.1 Protocol](http://www.w3.org/TR/sparql11-protocol/).
This requires an external triple store.

## Install

```bash
npm install --save rdf-store-sparql
```

## Usage

The constructor requires a `endpointUrl` parameter.
It must be a string pointing to the SPARQL endpoint.
Optional an `options` parameter can be given.

The `options` object can have the following properties:

- `updateUrl`: SPARQL endpoint URL for write operations.
  (Default `endpointUrl` parameter)
- `factory`: RDFJS data factory implementation.
  (Default the reference implementation `@rdfjs/data-model`)
- `maxQueryLength`: Max length of the `import` and `removeMatches` query.
  Multiple queries in sequence will be used if when `maxQueryLength` is reached.
  (Default `Infinity`)
