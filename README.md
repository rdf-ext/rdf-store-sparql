# rdf-store-sparql

[![Build Status](https://travis-ci.org/rdf-ext/rdf-store-sparql.svg?branch=master)](https://travis-ci.org/rdf-ext/rdf-store-sparql)
[![NPM Version](https://img.shields.io/npm/v/rdf-store-sparql.svg?style=flat)](https://npm.im/rdf-store-sparql)

SPARQL RDF Store that follows the [RDF Interface](http://bergos.github.io/rdf-ext-spec/) specification. Store implementation to access graphs using the [SPARQL 1.1 Protocol](http://www.w3.org/TR/sparql11-protocol/). This requires an external triple store.

## Install

```
npm install --save rdf-store-sparql
```

## Usage

The constructor accepts a single `options` parameters.

The `options` object can have the following properties:

* `endpointUrl` The URL of the SPARQL endpoint.
  This is a required property.
* `updateUrl` Use a different URL for write operations.
  By default the `endpointUrl` is used.
* `mimeType` Graph data is read using CONSTRUCT queries.
  This parameter defines the requested mime type.
  The default value is `text/turtle`.
* `serialize` Replaces the serialize function that is used to build the INSERT statements.
  The serialize function must generate valid SPARQL data!
  `rdf.serializeNTriples` is used by default.
* `parse` Replaces the function used to parse the CONSTRUCT query output.
  The parser must be able to parse data in the format defined in the `mimeType` property.
  `rdf.parseTurtle` is used by default.
* `request` Replaces the default request function.
  See the utils sections for implementations provided by RDF-Ext.

## History

Taken from [zazukoians/trifid-ld](https://github.com/zazukoians/trifid-ld)

## Licence

MIT
