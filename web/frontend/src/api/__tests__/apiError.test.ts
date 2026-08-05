import { describe, it, expect } from 'vitest'
import { ApiError } from '../client'

// A canned per-status message is fine when the server said nothing. It is
// actively harmful when the server did: a TurboSMS refusal arrives as 502
// with the reason in `detail`, and "Server is restarting" sends whoever reads
// it to check the wrong thing entirely.

function response(status: number, body?: unknown): Response {
  return new Response(body === undefined ? null : JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('ApiError.from', () => {
  it('prefers the server detail over the canned status text', async () => {
    const error = await ApiError.from(
      response(502, { detail: 'TurboSMS rejected the request: 103 INVALID_SENDER' }),
    )

    expect(error.message).toBe('TurboSMS rejected the request: 103 INVALID_SENDER')
    expect(error.status).toBe(502)
  })

  it('falls back to the canned text when there is no body', async () => {
    const error = await ApiError.from(response(502))

    expect(error.message).toBe('Server is restarting, please wait...')
  })

  it('falls back when the body is not JSON', async () => {
    const error = await ApiError.from(
      new Response('<html>502 Bad Gateway</html>', { status: 502 }),
    )

    expect(error.message).toBe('Server is restarting, please wait...')
  })

  it('ignores a non-string detail rather than rendering an object', async () => {
    // FastAPI validation errors put a list in `detail`; "[object Object]" in a
    // toast is worse than the generic message.
    const error = await ApiError.from(
      response(422, { detail: [{ loc: ['query', 'phone'], msg: 'too short' }] }),
    )

    expect(error.message).toBe('API error: ')
  })

  it('keeps the severity code the status implies', async () => {
    const server = await ApiError.from(response(502, { detail: 'gateway said no' }))
    const client = await ApiError.from(response(400, { detail: 'bad phone' }))

    expect(server.code).toBe('SERVER_ERROR')
    expect(client.code).toBe('CLIENT_ERROR')
  })
})
