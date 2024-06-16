import { EventEmitter } from 'events'
import toNT from '@rdfjs/to-ntriples'
import PromiseQueue from 'promise-queue'

class QuadStreamQuery extends EventEmitter {
  constructor ({
    callback,
    client,
    factory,
    input,
    maxQueryLength
  }) {
    super()

    this.callback = callback
    this.client = client
    this.factory = factory
    this.queryBegin = null
    this.queryEnd = null
    this.maxQueryLength = maxQueryLength
    this.lastGraph = null
    this.query = ''
    this.queue = new PromiseQueue(1, Infinity)
    this.error = null
    this.finished = false

    input.on('data', quad => this.push(quad))
    input.on('end', () => this.end())
    input.on('error', err => this.emit('error', err))
  }

  push (quad) {
    if (!quad.graph.equals(this.lastGraph)) {
      this.begin(quad.graph)

      this.lastGraph = quad.graph
    }

    const nt = toNT(this.factory.quad(quad.subject, quad.predicate, quad.object))

    if (this.queryBegin.length + nt.length + this.queryEnd.length > this.maxQueryLength) {
      this.error = new Error('quad is longer than maxQueryLength')

      return this.emit('error', this.error)
    }

    if (this.query.length + nt.length + 2 > this.maxQueryLength) {
      this.begin(quad.graph)
    }

    this.query += nt
  }

  begin (graph) {
    if (this.query) {
      this.query += this.queryEnd
      this.enqueue()
    }

    [this.queryBegin, this.queryEnd] = this.callback(graph)

    this.query = this.queryBegin
  }

  enqueue () {
    this.queue.add(this.buildQueryPromise(this.query)).then(() => {
      if (this.finished && this.queue.getQueueLength() + this.queue.getPendingLength() === 0) {
        return this.emit('end')
      }
    }).catch(err => {
      this.error = err

      this.emit('error', err)
    })

    this.query = ''
  }

  buildQueryPromise (query) {
    return async () => this.client.query.update(query)
  }

  end () {
    if (this.query) {
      this.query += this.queryEnd
      this.enqueue()
    }

    this.finished = true
  }
}

export default QuadStreamQuery
