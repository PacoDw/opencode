import { ClientFile } from "@opencode/core/client-file"
import { Session } from "@opencode/core/session"
import { ClientFileRequestNotFoundError } from "@opencode/protocol/errors"
import { Effect } from "effect"
import { HttpApiBuilder, HttpApiSchema } from "effect/http-api"
import { Api } from "../api"
import { missingSession } from "./session-error"

function missingRequest(requestID: ClientFile.ID) {
  return new ClientFileRequestNotFoundError({ requestID, message: `Client file request not found: ${requestID}` })
}

export const ClientFileHandler = HttpApiBuilder.group(Api, "server.clientFile", (handlers) =>
  Effect.gen(function* () {
    const clientFile = yield* ClientFile.Service
    const sessions = yield* Session.Service

    return handlers
      .handle(
        "session.clientFile.attach",
        Effect.fn(function* (ctx) {
          yield* sessions.get(ctx.params.sessionID).pipe(Effect.catchTag("Session.NotFoundError", missingSession))
          yield* clientFile.attach({ sessionID: ctx.params.sessionID, roots: ctx.payload.roots })
          return HttpApiSchema.NoContent.make()
        }),
      )
      .handle(
        "session.clientFile.detach",
        Effect.fn(function* (ctx) {
          yield* clientFile.detach(ctx.params.sessionID)
          return HttpApiSchema.NoContent.make()
        }),
      )
      .handle(
        "session.clientFile.reply",
        Effect.fn(function* (ctx) {
          const request = yield* clientFile.get(ctx.params.requestID)
          if (request?.sessionID !== ctx.params.sessionID) return yield* missingRequest(ctx.params.requestID)
          yield* clientFile
            .reply({ requestID: ctx.params.requestID, error: ctx.payload.error })
            .pipe(Effect.catchTag("ClientFile.NotFoundError", () => missingRequest(ctx.params.requestID)))
          return HttpApiSchema.NoContent.make()
        }),
      )
  }),
)
