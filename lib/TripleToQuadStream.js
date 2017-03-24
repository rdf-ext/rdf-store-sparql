const Transform = require('readable-stream').Transform

class TripleToQuadStream extends Transform {
  constructor (factory, graph) {
    super()

    this._writableState.objectMode = true
    this._readableState.objectMode = true

    this.factory = factory
    this.graph = graph
  }

  _transform (quad, encoding, done) {
    if (this.graph) {
      this.push(this.factory.quad(quad.subject, quad.predicate, quad.object, this.graph))
    } else {
      this.push(quad)
    }

    done()
  }
}

module.exports = TripleToQuadStream
