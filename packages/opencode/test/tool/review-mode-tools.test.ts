import { afterEach, describe, expect } from "bun:test"
import path from "path"
import fs from "fs/promises"
import { Effect } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { EditTool } from "../../src/tool/edit"
import { WriteTool } from "../../src/tool/write"
import { ApplyPatchTool } from "../../src/tool/apply_patch"
import { disposeAllInstances, TestInstance } from "../fixture/fixture"
import { LSP } from "@/lsp/lsp"
import { Format } from "../../src/format"
import { Agent } from "../../src/agent/agent"
import { EventV2Bridge } from "../../src/event-v2-bridge"
import { Truncate } from "@/tool/truncate"
import { SessionID, MessageID } from "../../src/session/schema"
import { testEffect } from "../lib/effect"
import { ReviewFs } from "@/effect/review-fs-layer"
import { ReviewOverlay } from "@opencode-ai/core/review-overlay"
import { forceEnableForAcp, reset, setClientWriteTextFileSupported } from "@/acp/review-mode"
import { completedToolContent } from "@/acp/tool"

const baseCtx = {
  sessionID: SessionID.make("ses_review"),
  messageID: MessageID.make("msg_review"),
  callID: "",
  agent: "build",
  abort: AbortSignal.any([]),
  messages: [],
  metadata: () => Effect.void,
  ask: () => Effect.void,
}

const layer = LayerNode.compile(
  LayerNode.group([LSP.node, FSUtil.node, Format.node, EventV2Bridge.node, Truncate.node, Agent.node]),
)

const it = testEffect(layer)

// Same wiring as ToolRegistry: the tool captures the review wrapper when it is built.
const withReviewFs = <A, E, R>(tool: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const afs = yield* FSUtil.Service
    return yield* tool.pipe(Effect.provideService(FSUtil.Service, ReviewFs.wrap(afs)))
  })

afterEach(async () => {
  delete process.env.OPENCODE_ACP_REVIEW
  delete process.env.OPENCODE_CLIENT
  reset()
  await disposeAllInstances()
})

const enableReview = () => {
  process.env.OPENCODE_CLIENT = "acp"
  forceEnableForAcp()
  setClientWriteTextFileSupported(true)
  ReviewOverlay.setActiveSession("ses_review")
}

const readDisk = (filepath: string) => Effect.promise(() => fs.readFile(filepath, "utf-8").catch(() => ""))

describe("review mode tools", () => {
  it.instance("edit stages without disk write", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const edit = yield* withReviewFs(EditTool)
      const tool = yield* edit.init()

      const filepath = path.join(test.directory, "edit.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "before\n", "utf-8"))

      yield* tool.execute(
        {
          filePath: filepath,
          oldString: "before",
          newString: "after",
        },
        baseCtx,
      )

      expect(yield* readDisk(filepath)).toBe("before\n")
      const staged = ReviewOverlay.get(filepath)
      expect(staged && "content" in staged ? staged.content : undefined).toBe("after\n")
    }),
  )

  it.instance("write stages new files without disk write", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const write = yield* withReviewFs(WriteTool)
      const tool = yield* write.init()

      const filepath = path.join(test.directory, "new.txt")
      yield* tool.execute({ filePath: filepath, content: "created" }, baseCtx)

      expect(yield* readDisk(filepath)).toBe("")
      expect(ReviewOverlay.get(filepath)).toEqual({ content: "created" })
    }),
  )

  it.instance("apply_patch stages add/update and deletes on disk", () =>
    Effect.gen(function* () {
      enableReview()
      const test = yield* TestInstance
      const patch = yield* withReviewFs(ApplyPatchTool)
      const tool = yield* patch.init()

      const updatePath = path.join(test.directory, "update.txt")
      const deletePath = path.join(test.directory, "delete.txt")
      yield* Effect.promise(() => fs.writeFile(updatePath, "old\n", "utf-8"))
      yield* Effect.promise(() => fs.writeFile(deletePath, "gone\n", "utf-8"))

      const patchText = [
        "*** Begin Patch",
        "*** Add File: added.txt",
        "+added",
        "*** Update File: update.txt",
        "@@",
        "-old",
        "+new",
        "*** Delete File: delete.txt",
        "*** End Patch",
      ].join("\n")

      const result = yield* tool.execute({ patchText }, baseCtx)

      expect(yield* readDisk(path.join(test.directory, "added.txt"))).toBe("")
      expect(yield* readDisk(updatePath)).toBe("old\n")
      expect(yield* readDisk(deletePath)).toBe("")

      expect(ReviewOverlay.get(path.join(test.directory, "added.txt"))).toEqual({ content: "added\n" })
      expect(ReviewOverlay.get(updatePath)).toEqual({ content: "new\n" })

      // delete/move bypass staging (ACP has no deleteFile), so the output must
      // warn that those changes already hit disk and won't be undone on reject.
      expect(result.output).toContain("applied directly to disk")

      const content = completedToolContent("apply_patch", {
        status: "completed",
        input: { patchText },
        output: result.output,
        metadata: result.metadata,
      })
      expect(content.some((item) => item.type === "diff" && item.path === deletePath && item.newText === "")).toBe(true)
    }),
  )
})
