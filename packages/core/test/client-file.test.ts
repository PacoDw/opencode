import { describe, expect } from "bun:test"
import { Effect, Fiber, Queue, Schema } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Bus } from "@opencode/core/bus"
import { ClientFile } from "@opencode/core/client-file"
import { Database } from "@opencode/core/database/database"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionTable } from "@opencode/core/session/sql"
import { SessionStore } from "@opencode/core/session/store"
import { testEffect } from "./lib/effect"

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionStore.node, ClientFile.node])),
)

const root = AbsolutePath.make("/project")
const parentID = Session.ID.make("ses_client_file_parent")
const childID = Session.ID.make("ses_client_file_child")
const isWriteRequested = Schema.is(ClientFile.Event.WriteRequested)

// The client in these tests is the test itself: it takes each published request and replies.
const requests = Effect.gen(function* () {
  const bus = yield* Bus.Service
  const queue = yield* Queue.unbounded<ClientFile.WriteRequest>()
  const unsubscribe = yield* bus.listen((event) =>
    isWriteRequested(event) ? Queue.offer(queue, event.data).pipe(Effect.asVoid) : Effect.void,
  )
  yield* Effect.addFinalizer(() => unsubscribe)
  return queue
})

const insertSessions = Effect.gen(function* () {
  const { db } = yield* Database.Service
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: root, sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  const row = { project_id: Project.ID.global, directory: root, title: "test", version: "test", agent: "build" }
  yield* db
    .insert(SessionTable)
    .values([
      { ...row, id: parentID, slug: "parent" },
      { ...row, id: childID, slug: "child", parent_id: parentID },
    ])
    .run()
    .pipe(Effect.orDie)
})

const write = (path: string, content = "content", origin = parentID) =>
  Effect.gen(function* () {
    const clientFile = yield* ClientFile.Service
    return yield* clientFile.write({ owner: parentID, origin, path: AbsolutePath.make(path), content })
  })

describe("ClientFile", () => {
  it.effect("resolves the attached session as the owner of paths under its roots", () =>
    Effect.gen(function* () {
      const clientFile = yield* ClientFile.Service
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })

      expect(yield* clientFile.owner(parentID, "/project/src/index.ts")).toBe(parentID)
      expect(yield* clientFile.owner(parentID, "/project")).toBe(parentID)
    }),
  )

  it.effect("resolves an attached ancestor as the owner of a subagent's writes", () =>
    Effect.gen(function* () {
      yield* insertSessions
      const clientFile = yield* ClientFile.Service
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })

      expect(yield* clientFile.owner(childID, "/project/src/index.ts")).toBe(parentID)
    }),
  )

  it.effect("has no owner outside the roots or for sessions nobody attached", () =>
    Effect.gen(function* () {
      yield* insertSessions
      const clientFile = yield* ClientFile.Service

      expect(yield* clientFile.owner(childID, "/project/src/index.ts")).toBeUndefined()
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })
      expect(yield* clientFile.owner(parentID, "/elsewhere/index.ts")).toBeUndefined()
      expect(yield* clientFile.owner(parentID, "/project-other/index.ts")).toBeUndefined()
      expect(yield* clientFile.owner(Session.ID.make("ses_client_file_unknown"), "/project/a.ts")).toBeUndefined()
    }),
  )

  it.effect("publishes the write and completes it when the client replies", () =>
    Effect.gen(function* () {
      const queue = yield* requests
      const clientFile = yield* ClientFile.Service
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })

      const pending = yield* write("/project/a.ts", "after\n", childID).pipe(Effect.forkChild)
      const request = yield* Queue.take(queue)

      expect(request).toMatchObject({
        sessionID: parentID,
        origin: childID,
        path: "/project/a.ts",
        content: "after\n",
      })
      expect(request.id).toStartWith("cfw_")
      expect(yield* clientFile.get(request.id)).toEqual(request)
      yield* clientFile.reply({ requestID: request.id })
      yield* Fiber.join(pending)
      expect(yield* clientFile.get(request.id)).toBeUndefined()
    }),
  )

  it.effect("fails the write when the client reports an error", () =>
    Effect.gen(function* () {
      const queue = yield* requests
      const clientFile = yield* ClientFile.Service
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })

      const pending = yield* write("/project/a.ts").pipe(Effect.flip, Effect.forkChild)
      const request = yield* Queue.take(queue)
      yield* clientFile.reply({ requestID: request.id, error: "invalid path" })
      const error = yield* Fiber.join(pending)

      expect(error).toBeInstanceOf(ClientFile.RejectedError)
      expect(error.message).toContain("/project/a.ts")
      expect(error.message).toContain("invalid path")
    }),
  )

  it.effect("fails pending and later writes once the client detaches", () =>
    Effect.gen(function* () {
      const queue = yield* requests
      const clientFile = yield* ClientFile.Service
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })

      const pending = yield* write("/project/a.ts").pipe(Effect.flip, Effect.forkChild)
      yield* Queue.take(queue)
      yield* clientFile.detach(parentID)

      expect(yield* Fiber.join(pending)).toBeInstanceOf(ClientFile.DetachedError)
      expect(yield* write("/project/b.ts").pipe(Effect.flip)).toBeInstanceOf(ClientFile.DetachedError)
      expect(yield* clientFile.owner(parentID, "/project/a.ts")).toBeUndefined()
    }),
  )

  it.effect("forgets an interrupted write so a late reply finds nothing", () =>
    Effect.gen(function* () {
      const queue = yield* requests
      const clientFile = yield* ClientFile.Service
      yield* clientFile.attach({ sessionID: parentID, roots: [root] })

      const pending = yield* write("/project/a.ts").pipe(Effect.forkChild)
      const request = yield* Queue.take(queue)
      yield* Fiber.interrupt(pending)

      expect(yield* clientFile.get(request.id)).toBeUndefined()
      expect(yield* clientFile.reply({ requestID: request.id }).pipe(Effect.flip)).toBeInstanceOf(
        ClientFile.NotFoundError,
      )
    }),
  )
})
