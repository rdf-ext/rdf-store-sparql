const toNtriples = require('@rdfjs/to-ntriples')

function termPattern (term) {
  if (!term) {
    return null
  }

  if (term.termType !== 'NamedNode' && term.termType !== 'Literal') {
    return null
  }

  return toNtriples.termToNTriples(term)
}

function graphFilter (graph) {
  if (!graph) {
    return ['GRAPH?g{', '}']
  }

  if (graph.termType === 'DefaultGraph') {
    return ['', '']
  }

  if (graph.termType === 'NamedNode') {
    return [`GRAPH<${graph.value}>{`, '}']
  }

  throw new Error(`expected null or Term of type NamedNode or DefaultGraph got type ${graph.termType}`)
}

function triplePattern (subject, predicate, object) {
  return [
    termPattern(subject) || '?s',
    termPattern(predicate) || '?p',
    termPattern(object) || '?o'
  ].join('')
}

function quadFilter (subject, predicate, object, graph) {
  const [graphBegin, graphEnd] = graphFilter(graph)

  return [
    graphBegin,
    termPattern(subject) || '?s',
    termPattern(predicate) || '?p',
    termPattern(object) || '?o',
    graphEnd
  ].join('')
}

function graphFrame (prefix, graph) {
  if (graph.termType === 'DefaultGraph') {
    return [`${prefix}{`, '}']
  }

  if (graph.termType === 'NamedNode') {
    return [`${prefix}{GRAPH<${graph.value}>{`, '}}']
  }

  throw new Error(`expected Term of type NamedNode or DefaultGraph`)
}

class QueryBuilder {
  static delete (graph) {
    if (!graph) {
      throw new Error(`expected Term of type NamedNode or DefaultGraph`)
    }

    if (graph.termType === 'DefaultGraph') {
      return 'CLEAR GRAPH DEFAULT'
    }

    if (graph.termType === 'NamedNode') {
      return `CLEAR GRAPH<${graph.value}>`
    }

    throw new Error(`expected Term of type NamedNode or DefaultGraph got ${graph.termType}`)
  }

  static importFrame (graph) {
    return graphFrame('INSERT DATA', graph)
  }

  static match (subject, predicate, object, graph) {
    return `CONSTRUCT{${triplePattern(subject, predicate, object)}}{${quadFilter(subject, predicate, object, graph)}}`
  }

  static removeFrame (graph) {
    return graphFrame('DELETE DATA', graph)
  }

  static removeMatches (subject, predicate, object, graph) {
    return `DELETE WHERE{${quadFilter(subject, predicate, object, graph)}}`
  }
}

module.exports = QueryBuilder
