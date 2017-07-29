# rdf-store-sparql

[![Build Status](https://travis-ci.org/rdf-ext/rdf-store-sparql.svg?branch=master)](https://travis-ci.org/rdf-ext/rdf-store-sparql)
[![npm version](https://badge.fury.io/js/rdf-store-sparql.svg)](https://badge.fury.io/js/rdf-store-sparql)

SPARQL RDF Store that follows the [RDFJS Store interface](https://github.com/rdfjs/representation-task-force/) specification.
This store implementation allows accessing graphs using the [SPARQL 1.1 Protocol](http://www.w3.org/TR/sparql11-protocol/).
This requires an external triple store.

## Install

    npm install --save rdf-store-sparql

## Usage

The constructor accepts a single `options` parameters.

The `options` object can have the following properties:

* `endpointUrl` The URL of the SPARQL endpoint.
  This is a required property.
* `updateUrl` Use a different URL for write operations.
  By default the `endpointUrl` is used.
