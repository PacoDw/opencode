export * as ClientFile from "./client-file.js"

import { Schema } from "effect"
import { ephemeral, inventory } from "./event.js"
import { ascending } from "./identifier.js"
import { AbsolutePath, statics } from "./schema.js"
import { SessionID } from "./session-id.js"

export const ID = Schema.String.check(Schema.isStartingWith("cfw_")).pipe(
  Schema.brand("ClientFile.ID"),
  Schema.annotate({ identifier: "ClientFile.ID" }),
  statics((schema) => ({ create: () => schema.make("cfw_" + ascending()) })),
)
export type ID = typeof ID.Type

/** A text write that an attached client performs, so the change reaches the client's own review flow. */
export interface WriteRequest extends Schema.Schema.Type<typeof WriteRequest> {}
export const WriteRequest = Schema.Struct({
  id: ID,
  /** The Session the client attached. Writes from its subagents belong to it too. */
  sessionID: SessionID,
  /** The Session whose tool produced the write. */
  origin: SessionID,
  path: AbsolutePath,
  content: Schema.String,
}).annotate({ identifier: "ClientFile.WriteRequest" })

const WriteRequested = ephemeral({ type: "client-file.write.requested", schema: WriteRequest.fields })

export const Event = { WriteRequested, Definitions: inventory(WriteRequested) }
