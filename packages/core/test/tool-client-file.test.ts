import fs from "fs/promises"
import path from "path"
import { describe, expect } from "bun:test"
import { Effect, Layer, Queue, Schema } from "effect"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { makeLocationNode } from "@opencode/util/effect/app-node"
import { Bus } from "@opencode/core/bus"
import { ClientFile } from "@opencode/core/client-file"
import { Database } from "@opencode/core/database/database"
import { Environment } from "@opencode/core/environment/index"
import { FileAccess } from "@opencode/core/file-access"
import { FileMutation } from "@opencode/core/file-mutation"
import { Formatter } from "@opencode/core/formatter"
import { Location } from "@opencode/core/location"
import { Permission } from "@opencode/core/permission"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionStore } from "@opencode/core/session/store"
import { Tool } from "@opencode/core/tool"
import { EditTool } from "@opencode/core/tool/plugin/edit"
import { PatchTool } from "@opencode/core/tool/plugin/patch"
import { WriteTool } from "@opencode/core/tool/plugin/write"
import { transformEnvironmentFiles } from "./fixture/environment"
import { location } from "./fixture/location"
import { withTempDir } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"
import { permissionLayer } from "./lib/permission"
import { executeTool, registerToolPlugin, toolIdentity } from "./lib/tool"

const deps = [
  Tool.node,
  FileAccess.node,
  FileMutation.node,
  Environment.node,
  Formatter.node,
  Location.node,
  Permission.node,
] as const
const editToolNode = makeLocationNode({
  name: "test/client-file-edit",
  layer: Layer.effectDiscard(registerToolPlugin(EditTool.Plugin)),
  deps,
})
const writeToolNode = makeLocationNode({
  name: "test/client-file-write",
  layer: Layer.effectDiscard(registerToolPlugin(WriteTool.Plugin)),
  deps,
})
const patchToolNode = makeLocationNode({
  name: "test/client-file-patch",
  layer: Layer.effectDiscard(registerToolPlugin(PatchTool.Plugin)),
  deps,
})

const sessionID = Session.ID.make("ses_client_file_tools")
const isWriteRequested = Schema.is(ClientFile.Event.WriteRequested)
const reviewWarning = "rejecting the review does not undo them"

const it = testEffect(Layer.empty)

/** What opencode itself did to the disk, and which files its formatter touched. */
const makeLocal = () => ({ writes: [] as string[], formatted: [] as string[] })

const provide = (directory: string, local: ReturnType<typeof makeLocal>) =>
  Effect.provide(
    AppNodeBuilder.build(
      LayerNode.group([
        Database.node,
        Bus.node,
        SessionStore.node,
        ClientFile.node,
        Tool.node,
        FileAccess.node,
        FileMutation.node,
        editToolNode,
        writeToolNode,
        patchToolNode,
      ]),
      [
        Environment.node.replace(
          transformEnvironmentFiles((files) => ({
            write: (target, content) =>
              Effect.sync(() => local.writes.push(target)).pipe(Effect.andThen(files.write(target, content))),
          })),
        ),
        Location.node.replace(
          Layer.succeed(Location.Service, Location.Service.of(location({ directory: AbsolutePath.make(directory) }))),
        ),
        Formatter.node.replace(
          Layer.mock(Formatter.Service, {
            file: (target) => Effect.sync(() => local.formatted.push(target)).pipe(Effect.as(false)),
          }),
        ),
        Permission.node.replace(permissionLayer({ assert: () => Effect.void })),
      ],
    ),
  )

/**
 * The editor side of the protocol: it receives each write, saves it the way the editor would (`save` may
 * reformat it), and replies. Returning a string from `save` rejects the write with that message.
 */
const startClient = (save: (request: ClientFile.WriteRequest) => Effect.Effect<string | undefined>) =>
  Effect.gen(function* () {
    const bus = yield* Bus.Service
    const clientFile = yield* ClientFile.Service
    const queue = yield* Queue.unbounded<ClientFile.WriteRequest>()
    const received: ClientFile.WriteRequest[] = []
    const unsubscribe = yield* bus.listen((event) =>
      isWriteRequested(event) ? Queue.offer(queue, event.data).pipe(Effect.asVoid) : Effect.void,
    )
    yield* Effect.addFinalizer(() => unsubscribe)
    yield* Queue.take(queue).pipe(
      Effect.tap((request) => Effect.sync(() => received.push(request))),
      Effect.flatMap((request) =>
        save(request).pipe(
          Effect.flatMap((error) => clientFile.reply({ requestID: request.id, error })),
          Effect.ignore,
        ),
      ),
      Effect.forever,
      Effect.forkScoped,
    )
    return received
  })

const saveToDisk =
  (format: (content: string) => string = (content) => content) =>
  (request: ClientFile.WriteRequest) =>
    Effect.promise(() => fs.writeFile(request.path, format(request.content))).pipe(Effect.as(undefined))

const attach = (root: string) =>
  Effect.gen(function* () {
    const clientFile = yield* ClientFile.Service
    yield* clientFile.attach({ sessionID, roots: [AbsolutePath.make(root)] })
  })

const run = (name: string, input: unknown, id = `call-${name}`) =>
  Effect.gen(function* () {
    const registry = yield* Tool.Service
    return yield* executeTool(registry, { sessionID, ...toolIdentity, call: { type: "tool-call", id, name, input } })
  })

const read = (file: string) => Effect.promise(() => fs.readFile(file, "utf8"))
const exists = (file: string) =>
  Effect.promise(() =>
    fs.access(file).then(
      () => true,
      () => false,
    ),
  )

describe("tool writes through an attached client", () => {
  it.live("edit sends its result to the client and reports what the client saved", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      const target = path.join(tmp.path, "hello.txt")
      return Effect.gen(function* () {
        yield* Effect.promise(() => fs.writeFile(target, "before\nrest\n"))
        const received = yield* startClient(saveToDisk((content) => content.replace("after", "AFTER")))
        yield* attach(tmp.path)

        const settled = yield* run("edit", { path: "hello.txt", oldString: "before", newString: "after" })

        expect(settled.status).toBe("completed")
        expect(received).toMatchObject([{ sessionID, origin: sessionID, path: target, content: "after\nrest\n" }])
        expect(local.writes).toEqual([])
        expect(local.formatted).toEqual([])
        expect(settled.output?.files[0]?.patch).toContain("-before\n+AFTER")
        expect(yield* read(target)).toBe("AFTER\nrest\n")
      }).pipe(provide(tmp.path, local))
    }),
  )

  it.live("edit fails and leaves the file unchanged when the client rejects the write", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      const target = path.join(tmp.path, "hello.txt")
      return Effect.gen(function* () {
        yield* Effect.promise(() => fs.writeFile(target, "before\n"))
        yield* startClient(() => Effect.succeed("the user rejected the change"))
        yield* attach(tmp.path)

        const settled = yield* run("edit", { path: "hello.txt", oldString: "before", newString: "after" })

        expect(settled.status).toBe("error")
        expect(settled.error?.message).toContain("the user rejected the change")
        expect(local.writes).toEqual([])
        expect(yield* read(target)).toBe("before\n")
      }).pipe(provide(tmp.path, local))
    }),
  )

  it.live("write lets the client create a new file", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      const target = path.join(tmp.path, "src", "created.txt")
      return Effect.gen(function* () {
        const received = yield* startClient((request) =>
          Effect.promise(async () => {
            await fs.mkdir(path.dirname(request.path), { recursive: true })
            await fs.writeFile(request.path, request.content)
          }).pipe(Effect.as(undefined)),
        )
        yield* attach(tmp.path)

        const settled = yield* run("write", { path: "src/created.txt", content: "created\n" })

        expect(settled.status).toBe("completed")
        expect(settled.output).toEqual({
          operation: "write",
          target,
          resource: "src/created.txt",
          existed: false,
        })
        expect(received).toMatchObject([{ path: target, content: "created\n" }])
        expect(local.writes).toEqual([])
        expect(local.formatted).toEqual([])
        expect(yield* read(target)).toBe("created\n")
      }).pipe(provide(tmp.path, local))
    }),
  )

  it.live("patch sends added and updated files to the client and deletes locally, with a warning", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      return Effect.gen(function* () {
        yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, "update.txt"), "old\n"))
        yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, "remove.txt"), "gone\n"))
        const received = yield* startClient(saveToDisk())
        yield* attach(tmp.path)

        const settled = yield* run("patch", {
          patchText: [
            "*** Begin Patch",
            "*** Add File: added.txt",
            "+added",
            "*** Update File: update.txt",
            "@@",
            "-old",
            "+new",
            "*** Delete File: remove.txt",
            "*** End Patch",
          ].join("\n"),
        })

        expect(settled.status).toBe("completed")
        expect(received.map((request) => [path.basename(request.path), request.content])).toEqual([
          ["added.txt", "added\n"],
          ["update.txt", "new\n"],
        ])
        expect(local.writes).toEqual([])
        expect(local.formatted).toEqual([])
        expect(yield* exists(path.join(tmp.path, "remove.txt"))).toBe(false)
        expect(yield* read(path.join(tmp.path, "update.txt"))).toBe("new\n")
        expect(settled.content?.[0]).toMatchObject({ type: "text", text: expect.stringContaining(reviewWarning) })
      }).pipe(provide(tmp.path, local))
    }),
  )

  it.live("patch sends a moved file's destination to the client and removes the source locally", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      return Effect.gen(function* () {
        yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, "source.txt"), "before\n"))
        const received = yield* startClient(saveToDisk())
        yield* attach(tmp.path)

        const settled = yield* run("patch", {
          patchText: [
            "*** Begin Patch",
            "*** Update File: source.txt",
            "*** Move to: moved.txt",
            "@@",
            "-before",
            "+after",
            "*** End Patch",
          ].join("\n"),
        })

        expect(settled.status).toBe("completed")
        expect(received).toMatchObject([{ path: path.join(tmp.path, "moved.txt"), content: "after\n" }])
        expect(yield* exists(path.join(tmp.path, "source.txt"))).toBe(false)
        expect(yield* read(path.join(tmp.path, "moved.txt"))).toBe("after\n")
        expect(settled.content?.[0]).toMatchObject({ type: "text", text: expect.stringContaining(reviewWarning) })
      }).pipe(provide(tmp.path, local))
    }),
  )

  it.live("patch without an attached client writes locally and gives no review warning", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      return Effect.gen(function* () {
        yield* Effect.promise(() => fs.writeFile(path.join(tmp.path, "remove.txt"), "gone\n"))
        const received = yield* startClient(saveToDisk())

        const settled = yield* run("patch", {
          patchText: [
            "*** Begin Patch",
            "*** Add File: added.txt",
            "+added",
            "*** Delete File: remove.txt",
            "*** End Patch",
          ].join("\n"),
        })

        expect(settled.status).toBe("completed")
        expect(received).toEqual([])
        expect(local.writes).toEqual([path.join(tmp.path, "added.txt")])
        expect(settled.content?.[0]).toMatchObject({ type: "text", text: expect.not.stringContaining(reviewWarning) })
      }).pipe(provide(tmp.path, local))
    }),
  )
})

describe("FileMutation with a client attached", () => {
  it.live("writes locally without a session, outside the roots, for binary content, or for other sessions", () =>
    withTempDir((tmp) =>
      withTempDir((outside) => {
        const local = makeLocal()
        return Effect.gen(function* () {
          const received = yield* startClient(saveToDisk())
          yield* attach(tmp.path)
          const access = yield* FileAccess.Service
          const mutation = yield* FileMutation.Service
          const inside = yield* access.resolve({ path: "inside.txt" })
          const external = yield* access.resolve({ path: path.join(outside.path, "external.txt") })
          const other = Session.ID.make("ses_client_file_other")

          const results = [
            yield* mutation.write({ target: inside, content: "no session" }),
            yield* mutation.write({ target: external, content: "outside", sessionID }),
            yield* mutation.write({ target: inside, content: new TextEncoder().encode("binary"), sessionID }),
            yield* mutation.writeTextPreservingBom({ target: inside, content: "other session", sessionID: other }),
          ]

          expect(results.map((result) => result.delegated)).toEqual([undefined, undefined, undefined, undefined])
          expect(received).toEqual([])
          expect(local.writes).toHaveLength(4)
          expect(yield* read(inside.absolute)).toBe("other session")
          expect(yield* read(external.absolute)).toBe("outside")
        }).pipe(provide(tmp.path, local))
      }),
    ),
  )

  it.live("marks a write the client performed as delegated", () =>
    withTempDir((tmp) => {
      const local = makeLocal()
      return Effect.gen(function* () {
        yield* startClient(saveToDisk())
        yield* attach(tmp.path)
        const access = yield* FileAccess.Service
        const mutation = yield* FileMutation.Service
        const target = yield* access.resolve({ path: "inside.txt" })

        expect(yield* mutation.write({ target, content: "through the client", sessionID })).toEqual({
          operation: "write",
          target: target.absolute,
          resource: "inside.txt",
          existed: false,
          delegated: true,
        })
        expect(local.writes).toEqual([])
        expect(yield* read(target.absolute)).toBe("through the client")
      }).pipe(provide(tmp.path, local))
    }),
  )
})
