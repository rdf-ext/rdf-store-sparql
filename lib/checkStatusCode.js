function checkStatusCode (res) {
  if (res.status >= 400) {
    const err = new Error('http error: ' + res.statusText)

    err.status = res.status

    return Promise.reject(err)
  }

  return Promise.resolve(res)
}

module.exports = checkStatusCode
