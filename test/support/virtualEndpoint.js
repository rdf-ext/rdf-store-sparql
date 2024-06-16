import nock from 'nock'
import SparqlStore from '../../index.js'

function virtualEndpoint ({
  content,
  count = 1,
  headers = {},
  id,
  maxQueryLength,
  method = 'GET',
  statusCode
} = {}) {
  const result = {
    queries: [],
    touched: false
  }

  for (let i = 0; i < count; i++) {
    let request

    if (method === 'GET') {
      request = nock('http://example.org').get(new RegExp(`${id}.*`))
    } else if (method === 'POST') {
      request = nock('http://example.org').post(`${id}/update`)
    }

    request.reply((uri, body) => {
      const searchParams = new URL(uri, 'http://localhost/').searchParams
      const bodyParams = body && new Map(body
        .split('&')
        .map(param => {
          const [key, value] = param.split('=')

          return [key, decodeURIComponent(value)]
        }))

      const params = bodyParams || searchParams

      result.touched = true

      if (params.has('query') || params.has('update')) {
        result.queries.push(params.get('query') || params.get('update'))
      }

      return [statusCode || (content ? 200 : 201), content, headers]
    })
  }

  const store = new SparqlStore({
    endpointUrl: `http://example.org${id}`,
    maxQueryLength,
    updateUrl: `http://example.org${id}/update`
  })

  return {
    result,
    store
  }
}

export default virtualEndpoint
