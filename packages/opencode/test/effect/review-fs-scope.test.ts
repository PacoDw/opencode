import { afterEach, describe, expect } from "bun:test"
import path from "path"
import fs from "fs/promises"
import { Effect } from "effect"
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
