import { ClientFile } from "@opencode/schema/client-file"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { Schema } from "effect"
import { HttpApiEndpoint, HttpApiGroup, HttpApiSchema, OpenApi } from "effect/http-api"
import { ClientFileRequestNotFoundError, SessionNotFoundError } from "../errors.js"

export const ClientFileGroup = HttpApiGroup.make("server.clientFile")
  .add(
    HttpApiEndpoint.put("session.clientFile.attach", "/api/session/:sessionID/client-file", {
      params: { sessionID: Session.ID },
      payload: Schema.Struct({ roots: Schema.Array(AbsolutePath) }),
      success: HttpApiSchema.NoContent,
      error: SessionNotFoundError,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "session.clientFile.attach",
        summary: "Attach a file-writing client",
        description:
          "Route the session's text file writes under the given roots, including its subagents' writes, through `client-file.write.requested` events instead of the local filesystem.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.delete("session.clientFile.detach", "/api/session/:sessionID/client-file", {
      params: { sessionID: Session.ID },
      success: HttpApiSchema.NoContent,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "session.clientFile.detach",
        summary: "Detach a file-writing client",
        description: "Stop routing the session's writes through the client and fail its pending write requests.",
      }),
    ),
  )
  .add(
    HttpApiEndpoint.post("session.clientFile.reply", "/api/session/:sessionID/client-file/:requestID/reply", {
      params: { sessionID: Session.ID, requestID: ClientFile.ID },
      payload: Schema.Struct({ error: Schema.String.pipe(Schema.optional) }),
      success: HttpApiSchema.NoContent,
      error: ClientFileRequestNotFoundError,
    }).annotateMerge(
      OpenApi.annotations({
        identifier: "session.clientFile.reply",
        summary: "Reply to a client file write request",
        description: "Report that the client wrote the file, or why it could not.",
      }),
    ),
  )
  .annotateMerge(OpenApi.annotations({ title: "clientFile", description: "Experimental client file routes." }))
