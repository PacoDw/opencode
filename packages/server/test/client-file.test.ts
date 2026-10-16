import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { Effect, Schema } from "effect"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })

const serve = Effect.gen(function* () {
  const handler = yield* ServerFetch.make({
    app: { version: "test" },
    database: { path: ":memory:" },
    fs: { filewatcher: false },
    models: { fetch: false },
  })
  return (path: string, method: string, body?: unknown) =>
    Effect.promise(async () => {
      const response = await handler(
        new Request(`http://opencode.local${path}`, {
          method,
          headers: { "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
        }),
      )
      const text = await response.text()
      return { status: response.status, body: text ? JSON.parse(text) : undefined }
    })
})

it.live("attaches and detaches a file-writing client for an existing session", () =>
  Effect.gen(function* () {
    const request = yield* serve
    const created = Schema.decodeUnknownSync(SessionResponse)((yield* request("/api/session", "POST", {})).body)

    expect(
      (yield* request(`/api/session/${created.data.id}/client-file`, "PUT", {
        roots: [created.data.location.directory],
      })).status,
    ).toBe(204)
    expect((yield* request(`/api/session/${created.data.id}/client-file`, "DELETE")).status).toBe(204)
  }).pipe(Effect.scoped),
)

it.live("rejects attaching to an unknown session and replying to an unknown request", () =>
  Effect.gen(function* () {
    const request = yield* serve
    const missing = yield* request("/api/session/ses_client_file_missing/client-file", "PUT", { roots: ["/project"] })
    expect(missing.status).toBe(404)
    expect(missing.body).toMatchObject({ _tag: "SessionNotFoundError" })

    const reply = yield* request("/api/session/ses_client_file_missing/client-file/cfw_missing/reply", "POST", {})
    expect(reply.status).toBe(404)
    expect(reply.body).toMatchObject({ _tag: "ClientFileRequestNotFoundError", requestID: "cfw_missing" })
  }).pipe(Effect.scoped),
)
