import { afterEach, describe, expect } from "bun:test"
import path from "path"
import fs from "fs/promises"
import { Cause, Effect, Exit } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Global } from "@opencode-ai/core/global"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { ReviewOverlay } from "@opencode-ai/core/review-overlay"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AppNodeBuilderV1 } from "@/effect/app-node-builder-v1"
import { Git } from "@/git"
import { Storage } from "@/storage/storage"
import { ToolRegistry } from "@/tool/registry"
import { forceEnableForAcp, reset, setClientWriteTextFileSupported } from "@/acp/review-mode"
import { MessageID, SessionID } from "@/session/schema"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

// Same shape as the production HTTP app graph: no FSUtil replacement. Review
// staging must come from ToolRegistry alone, so other services that share the
// graph keep writing to disk.
const it = testEffect(
  AppNodeBuilderV1.build(
    LayerNode.group([Storage.node, ToolRegistry.node, FSUtil.node, CrossSpawnSpawner.node, Ripgrep.node, Git.node]),
  ),
)

const ctx = {
  sessionID: SessionID.make("ses_review_scope"),
  messageID: MessageID.make("msg_review_scope"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

const enableReview = () => {
  process.env.OPENCODE_CLIENT = "acp"
  forceEnableForAcp()
  setClientWriteTextFileSupported(true)
  ReviewOverlay.setActiveSession("ses_review_scope")
}

const registryTool = (id: string) =>
  Effect.gen(function* () {
    const registry = yield* ToolRegistry.Service
    const tools = yield* registry.tools({
      providerID: ProviderV2.ID.opencode,
      modelID: ModelV2.ID.make("test"),
      agent: { name: "build", mode: "primary", permission: [], options: {} },
    })
    const tool = tools.find((item) => item.id === id)
    if (!tool) throw new Error(`${id} tool not registered`)
    return tool
  })

const readDisk = (file: string) => Effect.promise(() => fs.readFile(file, "utf-8"))

afterEach(async () => {
  delete process.env.OPENCODE_CLIENT
  reset()
  await disposeAllInstances()
})

describe("ReviewFs scope", () => {
  it.live("Storage.write reaches disk and never enters the overlay", () =>
    Effect.gen(function* () {
      enableReview()
      const storage = yield* Storage.Service
      const key = ["review_fs_scope", crypto.randomUUID()]
      const target = path.join(Global.Path.data, "storage", ...key) + ".json"

      yield* storage.write(key, { hello: "world" })

      expect(yield* readDisk(target)).toContain("world")
      expect(ReviewOverlay.get(target)).toBeUndefined()
      expect(ReviewOverlay.drainPendingWrites().map((item) => item.path)).not.toContain(target)
      yield* Effect.promise(() => fs.rm(path.dirname(target), { recursive: true, force: true }))
    }),
  )

  it.instance("edit from the registry is staged and leaves disk untouched", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const edit = yield* registryTool("edit")
      const file = path.join(test.directory, "scope-edit.txt")
      yield* Effect.promise(() => fs.writeFile(file, "before\n", "utf-8"))

      yield* edit.execute({ filePath: file, oldString: "before", newString: "after" }, ctx)

      expect(yield* readDisk(file)).toBe("before\n")
      const staged = ReviewOverlay.get(file)
      expect(staged && "content" in staged ? staged.content : undefined).toBe("after\n")
    }),
  )

  it.instance("read from the registry returns staged content after an edit", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const edit = yield* registryTool("edit")
      const read = yield* registryTool("read")
      const file = path.join(test.directory, "scope-read.txt")
      yield* Effect.promise(() => fs.writeFile(file, "before\n", "utf-8"))

      yield* edit.execute({ filePath: file, oldString: "before", newString: "after" }, ctx)
      const result = yield* read.execute({ filePath: file }, ctx)

      expect(yield* readDisk(file)).toBe("before\n")
      expect(result.output).toContain("1: after")
      expect(result.output).not.toContain("before")
    }),
  )

  it.instance("read from the registry opens a file that was only staged by write", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const write = yield* registryTool("write")
      const read = yield* registryTool("read")
      const file = path.join(test.directory, "scope-new.txt")

      yield* write.execute({ filePath: file, content: "created\n" }, ctx)
      const result = yield* read.execute({ filePath: file }, ctx)

      expect(
        yield* Effect.promise(() =>
          fs.access(file).then(
            () => true,
            () => false,
          ),
        ),
      ).toBe(false)
      expect(result.output).toContain("1: created")
    }),
  )

  // Reproduces a real Windsurf session: the client writes each staged edit to
  // disk, the agent then truncates the file through the shell, and its next
  // edit must apply to the truncated file instead of the stale staged copy.
  it.instance("edit after an external truncation applies to the disk, not the stale staged copy", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const edit = yield* registryTool("edit")
      const file = path.join(test.directory, "scope-truncated.rs")
      yield* Effect.promise(() => fs.writeFile(file, "impl\nold tests\n", "utf-8"))

      yield* edit.execute({ filePath: file, oldString: "old tests", newString: "old tests v2" }, ctx)
      const staged = ReviewOverlay.get(file)
      const applied = staged && "content" in staged ? staged.content : ""
      yield* Effect.promise(() => fs.writeFile(file, applied, "utf-8"))
      yield* Effect.promise(() => fs.writeFile(file, "impl\n", "utf-8"))

      yield* edit.execute({ filePath: file, oldString: "impl\n", newString: "impl\nnew tests\n" }, ctx)

      const result = ReviewOverlay.get(file)
      expect(result && "content" in result ? result.content : undefined).toBe("impl\nnew tests\n")
    }),
  )

  it.instance("write over a file that is only staged sees it as existing", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const write = yield* registryTool("write")
      const file = path.join(test.directory, "scope-rewrite.txt")

      yield* write.execute({ filePath: file, content: "v1\n" }, ctx)
      const result = yield* write.execute({ filePath: file, content: "v2\n" }, ctx)

      expect(result.metadata.exists).toBe(true)
      const staged = ReviewOverlay.get(file)
      expect(staged && "content" in staged ? staged.content : undefined).toBe("v2\n")
    }),
  )

  it.instance("edit with an empty oldString refuses to overwrite a file that is only staged", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const write = yield* registryTool("write")
      const edit = yield* registryTool("edit")
      const file = path.join(test.directory, "scope-create.txt")

      yield* write.execute({ filePath: file, content: "created\n" }, ctx)
      const exit = yield* edit
        .execute({ filePath: file, oldString: "", newString: "clobbered\n" }, ctx)
        .pipe(Effect.exit)

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(String(Cause.squash(exit.cause))).toContain("oldString cannot be empty")
      const staged = ReviewOverlay.get(file)
      expect(staged && "content" in staged ? staged.content : undefined).toBe("created\n")
    }),
  )

  it.instance("edit from the registry writes to disk when review mode is off", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const edit = yield* registryTool("edit")
      const file = path.join(test.directory, "scope-off.txt")
      yield* Effect.promise(() => fs.writeFile(file, "before\n", "utf-8"))

      yield* edit.execute({ filePath: file, oldString: "before", newString: "after" }, ctx)

      expect(yield* readDisk(file)).toBe("after\n")
      expect(ReviewOverlay.get(file)).toBeUndefined()
    }),
  )
})
