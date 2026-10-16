export * as FileMutation from "./file-mutation.js"

import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Context, Effect, Layer } from "effect"
import { KeyedMutex } from "./effect/keyed-mutex.js"
import { FSUtil } from "@opencode/util/fs-util"
import { Bom } from "@opencode/util/bom"
import { ClientFile } from "./client-file.js"
import { Environment } from "./environment/index.js"
import type { Files } from "./environment/index.js"
import type { FileAccess } from "./file-access.js"
import type { SessionSchema } from "./session/schema.js"

export type Target = Pick<FileAccess.Target, "absolute" | "resource">

export interface WriteInput {
  readonly target: Target
  readonly content: string | Uint8Array
  /** The Session whose tool writes. Text inside an attached client's roots goes through that client. */
  readonly sessionID?: SessionSchema.ID
}

export interface TextWriteInput {
  readonly target: Target
  readonly content: string
  /** The Session whose tool writes. Text inside an attached client's roots goes through that client. */
  readonly sessionID?: SessionSchema.ID
}

export interface WriteResult {
  readonly operation: "write"
  readonly target: string
  readonly resource: string
  readonly existed: boolean
  /** Present when the attached client wrote the file; the disk then holds whatever the client saved. */
  readonly delegated?: true
}

export type ClientError = ClientFile.RejectedError | ClientFile.DetachedError

export interface Interface {
  /** Serialize a complete read/prepare/write mutation transaction by resolved path. */
  readonly withLock: (
    targets: ReadonlyArray<string>,
  ) => <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  readonly write: (input: WriteInput) => Effect.Effect<WriteResult, Environment.Failed | ClientError>
  /** Write text while retaining an existing UTF-8 BOM and emitting at most one BOM. */
  readonly writeTextPreservingBom: (
    input: TextWriteInput,
  ) => Effect.Effect<WriteResult, Environment.WrongKind | Environment.Failed | ClientError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/FileMutation") {}

export const readText = Effect.fn("FileMutation.readText")(function* (files: Files, target: string) {
  return Bom.decodeBytes((yield* files.read(target)).bytes)
})

export const syncTextBom = Effect.fn("FileMutation.syncTextBom")(function* (
  files: Files,
  target: string,
  bom: boolean,
) {
  const synced = Bom.syncBytes((yield* files.read(target)).bytes, bom)
  if (synced.bytes) yield* files.write(target, synced.bytes)
  return synced.text
})

/** Share transaction locks across Location graphs that address the same file. */
const transactionLocks = KeyedMutex.makeUnsafe<string>()

/**
 * Mutation locking is process-local and serializes cooperating OpenCode
 * changes; external writes can still race.
 */
const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const environment = yield* Environment.Service
    const clientFile = yield* ClientFile.Service
    const locks = KeyedMutex.makeUnsafe<string>()
    const withLock: Interface["withLock"] = (targets) => (effect) =>
      [...new Set(targets.map(FSUtil.resolve))]
        .sort()
        .reduceRight((result, target) => transactionLocks.withLock(target)(result), effect)
    const withTargetLock =
      (target: Target) =>
      <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        locks.withLock(target.absolute)(Effect.uninterruptible(effect))

    const writeResult = (target: Target, existed: boolean): WriteResult => ({
      operation: "write",
      target: target.absolute,
      resource: target.resource,
      existed,
    })

    const exists = (target: Target) =>
      environment.files.stat(target.absolute).pipe(
        Effect.as(true),
        Effect.catchTag("Environment.NotFound", () => Effect.succeed(false)),
      )

    // The client reports success only after it saved the file, so later reads see what it wrote. Unlike a
    // local write, the wait stays interruptible: the user can cancel the turn while the client decides.
    const delegate = <E>(
      target: Target,
      owner: SessionSchema.ID,
      origin: SessionSchema.ID,
      prepare: Effect.Effect<{ readonly text: string; readonly existed: boolean }, E>,
    ) =>
      locks.withLock(target.absolute)(
        Effect.gen(function* () {
          const prepared = yield* prepare
          yield* clientFile.write({ owner, origin, path: target.absolute, content: prepared.text })
          return { ...writeResult(target, prepared.existed), delegated: true as const }
        }),
      )

    const write = Effect.fn("FileMutation.write")(function* (input: WriteInput) {
      const content = input.content
      if (typeof content === "string" && input.sessionID !== undefined) {
        const owner = yield* clientFile.owner(input.sessionID, input.target.absolute)
        if (owner !== undefined)
          return yield* delegate(
            input.target,
            owner,
            input.sessionID,
            exists(input.target).pipe(Effect.map((existed) => ({ text: content, existed }))),
          )
      }
      return yield* withTargetLock(input.target)(
        Effect.gen(function* () {
          const existed = yield* exists(input.target)
          yield* environment.files.write(
            input.target.absolute,
            typeof content === "string" ? new TextEncoder().encode(content) : content,
          )
          return writeResult(input.target, existed)
        }),
      )
    })

    const writeTextPreservingBom = Effect.fn("FileMutation.writeTextPreservingBom")(function* (input: TextWriteInput) {
      const prepare = Effect.gen(function* () {
        const next = Bom.split(input.content)
        const current = yield* environment.files.read(input.target.absolute, { offset: 0, length: 3 }).pipe(
          Effect.map((result) => result.bytes),
          Effect.catchTag("Environment.NotFound", () => Effect.undefined),
        )
        return {
          text: Bom.join(next.text, Boolean(current && Bom.has(current)) || next.bom),
          existed: current !== undefined,
        }
      })
      if (input.sessionID !== undefined) {
        const owner = yield* clientFile.owner(input.sessionID, input.target.absolute)
        if (owner !== undefined) return yield* delegate(input.target, owner, input.sessionID, prepare)
      }
      return yield* withTargetLock(input.target)(
        Effect.gen(function* () {
          const prepared = yield* prepare
          yield* environment.files.write(input.target.absolute, new TextEncoder().encode(prepared.text))
          return writeResult(input.target, prepared.existed)
        }),
      )
    })

    return Service.of({ withLock, write, writeTextPreservingBom })
  }),
)

export const node = makeLocationNode({ service: Service, layer, deps: [Environment.node, ClientFile.node] })

/**
 * Deferred until the corresponding integrations exist.
 */
// TODO: Publish watcher/file-edit events after watcher integration exists.
// TODO: Add snapshots / undo after snapshot design exists.
// TODO: Notify LSP and collect diagnostics after LSP runtime exists.
// TODO: Design multi-file transactions / rollback if patch needs atomic edits.
// Until then, edits are sequential and report partial application.
// TODO: Define crash recovery and idempotency for side effects between Tool.Called and durable settlement.
