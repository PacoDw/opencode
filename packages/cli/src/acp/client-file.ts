import { RequestError } from "@agentclientprotocol/sdk"
import type { OpenCodeClient, OpenCodeEvent } from "@opencode/client/effect"
import { AbsolutePath } from "@opencode/schema/schema"
import type { Session } from "@opencode/schema/session"
import { Cause, Deferred, Effect, Ref, type Scope, Stream } from "effect"
import type { Capabilities } from "./capabilities"
import { ACPClient } from "./client"
import type { ACPConnection } from "./connection"
import type { ACPError } from "./error"

type WriteRequest = Extract<OpenCodeEvent, { type: "client-file.write.requested" }>["data"]

export interface Interface {
  /**
   * Routes the session's text writes under `roots`, and its subagents' writes, through the client's
   * `fs/write_text_file` until the current scope closes, so the edits enter the client's review. A client
   * without that capability keeps local writes, and nothing changes.
   */
  readonly attach: (
    sessionID: Session.ID,
    roots: ReadonlyArray<string>,
  ) => Effect.Effect<void, ACPError.Error, Scope.Scope>
}

export const make = Effect.fnUntraced(function* (input: {
  readonly client: OpenCodeClient
  readonly connection: ACPConnection.Interface
  readonly capabilities: Ref.Ref<Capabilities>
}) {
  const scope = yield* Effect.scope
  const attached = new Set<string>()

  const write = (request: WriteRequest) =>
    input.connection.writeTextFile({ sessionId: request.sessionID, path: request.path, content: request.content }).pipe(
      Effect.as(undefined),
      // Any failure, a lost connection included, must reach the tool; an unanswered write never ends.
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : Effect.succeed(reason(cause)),
      ),
      Effect.flatMap((error) =>
        input.client.clientFile.reply({ sessionID: request.sessionID, requestID: request.id, error }),
      ),
      // The tool stopped waiting: it was interrupted, or the session detached.
      Effect.catchTag("ClientFileRequestNotFoundError", () => Effect.void),
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logWarning("ACP client file reply failed", cause),
      ),
    )

  // Started by the first attach, so a client that cannot write files keeps a single event stream.
  const listen = yield* Effect.cached(
    Effect.gen(function* () {
      const connected = yield* Deferred.make<void>()
      yield* input.client.event.subscribe().pipe(
        Stream.tap((event) =>
          event.type === "server.connected" ? Deferred.succeed(connected, undefined) : Effect.void,
        ),
        Stream.filter(
          (event): event is Extract<OpenCodeEvent, { type: "client-file.write.requested" }> =>
            event.type === "client-file.write.requested" && attached.has(event.data.sessionID),
        ),
        // A write the client is still showing must not hold up writes to other files.
        Stream.runForEach((event) => write(event.data).pipe(Effect.forkIn(scope), Effect.asVoid)),
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.logWarning("ACP client file event stream failed", cause),
        ),
        Effect.ensuring(Deferred.succeed(connected, undefined)),
        Effect.forkIn(scope),
      )
      // Attaching only after the stream is live means no write request can be missed.
      yield* Deferred.await(connected)
    }),
  )

  const detach = (sessionID: Session.ID) =>
    Effect.sync(() => attached.delete(sessionID)).pipe(
      Effect.andThen(input.client.clientFile.detach({ sessionID })),
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logWarning("ACP client file detach failed", cause),
      ),
    )

  return {
    attach: Effect.fnUntraced(function* (sessionID, roots) {
      if (!(yield* Ref.get(input.capabilities)).writeTextFile) return
      yield* listen
      attached.add(sessionID)
      yield* input.client.clientFile.attach({ sessionID, roots: roots.map((root) => AbsolutePath.make(root)) }).pipe(
        Effect.catch(ACPClient.classify),
        Effect.onError(() => Effect.sync(() => attached.delete(sessionID))),
      )
      yield* Effect.addFinalizer(() => detach(sessionID))
    }),
  } satisfies Interface
})

// The client's own message reaches the model, which can then decide how to continue.
function reason(cause: Cause.Cause<unknown>) {
  const error = Cause.squash(cause)
  if (!(error instanceof Error)) return String(error)
  if (!(error instanceof RequestError) || error.data === undefined) return error.message
  return `${error.message} (${typeof error.data === "string" ? error.data : JSON.stringify(error.data)})`
}

export * as ACPClientFile from "./client-file"
