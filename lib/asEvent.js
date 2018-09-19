const EventEmitter = require('events').EventEmitter

function asEvent (p) {
  const event = new EventEmitter()

  Promise.resolve().then(() => {
    return p()
  }).then(() => event.emit('end')).catch(err => event.emit('error', err))

  return event
}

module.exports = asEvent
