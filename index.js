'use strict';

//TODO: handle blank nodes
var SparqlStore = function (rdf, options) {
  var
    self = this;

  options = options || {};

  self.endpointUrl = options.endpointUrl;
  self.updateUrl = options.updateUrl || self.endpointUrl;
  self.mimeType = options.mimeType || 'text/turtle';
  self.serialize = options.serialize  || rdf.serializeNTriples;
  self.parse = options.parse || rdf.parseTurtle;
  self.request = options.request || rdf.defaultRequest;

  var httpSuccess = function (statusCode) {
    return (statusCode >= 200 && statusCode < 300);
  };

  var buildMatch = function (subject, predicate, object) {
    var match = '';

    var nodeToNT = function (node) {
      if (typeof node === 'string') {
        if (node.substr(0, 2) === '_:') {
          return node;
        } else {
          return '<' + node + '>';
        }
      }

      return node.toNT();
    };

    match += subject ? nodeToNT(subject) : '?s';
    match += predicate ? ' ' + nodeToNT(predicate) : ' ?p';
    match += object ? ' ' + nodeToNT(object) : ' ?o';

    return match;
  };

  self.executeQuery = function (queryString, callback, queryParams, requestOptions) {
    queryParams = queryParams || {parse: true};
    requestOptions = requestOptions || {};

    var queryUrl = self.endpointUrl + '?query=' + encodeURIComponent(queryString);
    var reqHeaders = { 'Accept': self.mimeType };

    self.request('GET', queryUrl, reqHeaders, null, function (statusCode, resHeaders, resContent, error) {
      // error during request
      if (error) {
        return callback('request error: ' + error);
      }

      // http status code != success
      if (!httpSuccess(statusCode)) {
        return callback('status code error: ' + statusCode);
      }

      if (statusCode === 200 && queryParams.parse) {
        self.parse(resContent, callback);
      } else {
        callback();
      }
    });
  };

  self.executeUpdateQuery = function (queryString, callback, queryParams, requestOptions) {
    queryParams = queryParams || {parse: true};
    requestOptions = requestOptions || {};

    var reqHeaders = { 'Content-Type': 'application/sparql-update' };

    self.request('POST', self.updateUrl, reqHeaders, queryString, function (statusCode, resHeaders, resContent, error) {
      // error during request
      if (error) {
        return callback('request error: ' + error);
      }

      // http status code != success
      if (!httpSuccess(statusCode)) {
        return callback('status code error: ' + statusCode);
      }

      if (statusCode === 200 && queryParams.parse) {
        self.parse(resContent, callback);
      } else {
        callback();
      }
    });
  };

  self.graph = function (graphIri, callback) {
    self.match(graphIri, null, null, null, callback);
  };

  self.match = function (graphIri, subject, predicate, object, callback, limit) {
    var filter = buildMatch(subject, predicate, object);
    var query = 'CONSTRUCT { ' + filter + ' } { GRAPH <' + graphIri + '> {' + filter + ' }}'; // TODO: use limit parameters

    self.executeQuery(query, callback);
  };

  self.add = function (graphIri, graph, callback) {
    self.serialize(graph, function (error, data) {
      if (error) {
        callback(error)
      } else {
        var query = '' +
          'DROP SILENT GRAPH <' + graphIri + '>;' +
          'INSERT DATA { GRAPH <' + graphIri + '> { ' + data + ' } }';

        self.executeUpdateQuery(query, function (error) {
          if (error) {
            callback(error);
          } else {
            callback(null, graph);
          }
        });
      }
    });
  };

  self.merge = function (graphIri, graph, callback) {
    self.serialize(graph, function (error, data) {
      if (error) {
        callback(error)
      } else {
        var query = 'INSERT DATA { GRAPH <' + graphIri + '> { ' + data +' } }';

        self.executeUpdateQuery(query, function (error) {
          if (error) {
            callback(error);
          } else {
            callback(null, graph);
          }
        });
      }
    });
  };

  self.remove = function (graphIri, graph, callback) {
    self.serialize(graph, function (error, data) {
      if (error) {
        callback(error)
      } else {
        var query = 'DELETE DATA FROM <' + graphIri + '> { ' + data + ' }';

        self.executeUpdateQuery(query, callback);
      }
    });
  };

  self.removeMatches = function (graphIri, subject, predicate, object, callback) {
    var query = 'DELETE FROM GRAPH <' + graphIri + '> WHERE { ' + buildMatch(subject, predicate, object) + ' }';

    self.executeUpdateQuery(query, callback);
  };

  self.delete = function (graphIri, callback) {
    var query = 'CLEAR  GRAPH <' + graphIri + '>';

    self.executeUpdateQuery(query, callback);
  };
};

module.exports = SparqlStore
