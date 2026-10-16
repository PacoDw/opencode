export * as ClientFile from "./client-file.js"

import { ClientFile } from "@opencode/schema/client-file"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { FSUtil } from "@opencode/util/fs-util"
import { Context, Deferred, Effect, Layer, Schema } from "effect"
import { Bus } from "./bus.js"
import type { AbsolutePath } from "./schema.js"
import { SessionSchema } from "./session/schema.js"
import { SessionStore } from "./session/store.js"

export const ID = ClientFile.ID
export type ID = ClientFile.ID

export const WriteRequest = ClientFile.WriteRequest
export type WriteRequest = ClientFile.WriteRequest

export { Event } from "@opencode/schema/client-file"

export class RejectedError extends Schema.TaggedError<RejectedError>()("ClientFile.RejectedError", {
  path: Schema.String,
  reason: Schema.String,
}) {
  override get message() {
    return `The client rejected the write to ${this.path}: ${this.reason}`
  }
}

export class DetachedError extends Schema.TaggedError<DetachedError>()("ClientFile.DetachedError", {
  sessionID: SessionSchema.ID,
}) {
  override get message() {
    return `The client detached from session ${this.sessionID} before writing`
  }
}

export class NotFoundError extends Schema.TaggedError<NotFoundError>()("ClientFile.NotFoundError", {
  requestID: ID,
}) {
  override get message() {
    return `Client file request not found: ${this.requestID}`
  }
}

export interface AttachInput {
  readonly sessionID: SessionSchema.ID
  /** Directories the client can write. Writes elsewhere stay local. */
  readonly roots: ReadonlyArray<AbsolutePath>
}

export interface WriteInput {
  /** The attached Session, as resolved by `owner`. */
  readonly owner: SessionSchema.ID
  /** The Session whose tool produced the write. */
  readonly origin: SessionSchema.ID
  readonly path: AbsolutePath
  readonly content: string
}

export interface ReplyInput {
  readonly requestID: ID
  /** The client's failure message. Absent when the client wrote the file. */
  readonly error?: string
}

export interface Interface {
  readonly attach: (input: AttachInput) => Effect.Effect<void>
  /** Fails the client's pending writes; later writes for the Session stay local. */
  readonly detach: (sessionID: SessionSchema.ID) => Effect.Effect<void>
  /** The Session itself or its nearest attached ancestor, when that Session's roots contain the path. */
  readonly owner: (sessionID: SessionSchema.ID, path: string) => Effect.Effect<SessionSchema.ID | undefined>
  /** Publishes the write for the attached client and waits until the client reports it written. */
  readonly write: (input: WriteInput) => Effect.Effect<void, RejectedError | DetachedError>
  readonly reply: (input: ReplyInput) => Effect.Effect<void, NotFoundError>
  readonly get: (requestID: ID) => Effect.Effect<WriteRequest | undefined>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/ClientFile") {}

interface Pending {
  readonly request: WriteRequest
  readonly deferred: Deferred.Deferred<void, RejectedError | DetachedError>
}

// Process-global and Session-ID based, so a registration survives Location invalidation.
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const sessions = yield* SessionStore.Service
    const attached = new Map<SessionSchema.ID, ReadonlyArray<string>>()
    const pending = new Map<ID, Pending>()

    const ownerOf = (sessionID: SessionSchema.ID, path: string): Effect.Effect<SessionSchema.ID | undefined> =>
      Effect.gen(function* () {
        const roots = attached.get(sessionID)
        if (roots) return roots.some((root) => FSUtil.contains(root, path)) ? sessionID : undefined
        const parentID = (yield* sessions.get(sessionID))?.parentID
        return parentID === undefined ? undefined : yield* ownerOf(parentID, path)
      })

    const owner = Effect.fn("ClientFile.owner")(function* (sessionID: SessionSchema.ID, path: string) {
      // Without an attached client there is no owner, so skip the ancestor lookups.
      if (attached.size === 0) return undefined
      return yield* ownerOf(sessionID, path)
    })

    const write = Effect.fn("ClientFile.write")(function* (input: WriteInput) {
      if (!attached.has(input.owner)) return yield* new DetachedError({ sessionID: input.owner })
      const request: WriteRequest = {
        id: ID.create(),
        sessionID: input.owner,
        origin: input.origin,
        path: input.path,
        content: input.content,
      }
      const deferred = yield* Deferred.make<void, RejectedError | DetachedError>()
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.sync(() => pending.set(request.id, { request, deferred })).pipe(
          Effect.andThen(bus.publish(ClientFile.Event.WriteRequested, request)),
          Effect.andThen(restore(Deferred.await(deferred))),
          Effect.ensuring(Effect.sync(() => pending.delete(request.id))),
        ),
      )
    })

    const detach = Effect.fn("ClientFile.detach")(function* (sessionID: SessionSchema.ID) {
      attached.delete(sessionID)
      const detached = Array.from(pending.values()).filter((item) => item.request.sessionID === sessionID)
      detached.forEach((item) => pending.delete(item.request.id))
      yield* Effect.forEach(detached, (item) => Deferred.fail(item.deferred, new DetachedError({ sessionID })), {
        discard: true,
      })
    })

    const reply = Effect.fn("ClientFile.reply")(function* (input: ReplyInput) {
      const item = pending.get(input.requestID)
      if (!item) return yield* new NotFoundError({ requestID: input.requestID })
      pending.delete(input.requestID)
      yield* input.error === undefined
        ? Deferred.succeed(item.deferred, undefined)
        : Deferred.fail(item.deferred, new RejectedError({ path: item.request.path, reason: input.error }))
    })

    return Service.of({
      // Tools compare paths without following symlinks, so both spellings of a root count.
      attach: (input) =>
        Effect.sync(() => {
          attached.set(input.sessionID, [...new Set(input.roots.flatMap((root) => [root, FSUtil.resolve(root)]))])
        }),
      detach,
      owner,
      write,
      reply,
      get: (requestID) => Effect.sync(() => pending.get(requestID)?.request),
    })
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [Bus.node, SessionStore.node] })
