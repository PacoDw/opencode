import { afterEach, describe, expect } from "bun:test"
import fs from "fs/promises"
import path from "path"
import { Effect, Option, Stream } from "effect"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ReviewOverlay } from "@opencode-ai/core/review-overlay"
import { ReviewFs } from "@/effect/review-fs-layer"
import { TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"

const it = testEffect(ReviewFs.defaultLayer)

afterEach(() => {
  ReviewOverlay.reset()
})

const readDisk = (filepath: string) => Effect.promise(() => fs.readFile(filepath, "utf-8").catch(() => ""))

describe("ReviewFs overlay", () => {
  it.instance("stages writes without mutating disk", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)
      ReviewOverlay.setActiveSession("sess-1")

      const filepath = path.join(test.directory, "staged.txt")
      yield* afs.writeWithDirs(filepath, "staged content")

      expect(yield* readDisk(filepath)).toBe("")
      expect(yield* afs.readFileString(filepath)).toBe("staged content")
      expect(yield* afs.exists(filepath)).toBe(true)
      expect(yield* afs.existsSafe(filepath)).toBe(true)
      expect(yield* afs.isFile(filepath)).toBe(true)
      const info = yield* afs.stat(filepath)
      expect(info.type).toBe("File")
    }),
  )

  it.instance("read-after-write returns latest staged content", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "twice.txt")
      yield* afs.writeWithDirs(filepath, "first")
      yield* afs.writeWithDirs(filepath, "second")
      expect(yield* afs.readFileString(filepath)).toBe("second")
      expect(yield* readDisk(filepath)).toBe("")
    }),
  )

  it.instance("readFile returns staged bytes for Bom.readFile", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "bytes.txt")
      yield* afs.writeWithDirs(filepath, "utf8 body")
      const bytes = yield* afs.readFile(filepath)
      expect(new TextDecoder().decode(bytes)).toBe("utf8 body")
    }),
  )

  it.instance("markDeleted hides overlay paths", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "gone.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "on disk", "utf-8"))
      ReviewOverlay.markDeleted(filepath)

      expect(yield* afs.exists(filepath)).toBe(false)
      expect(yield* afs.existsSafe(filepath)).toBe(false)
      expect(yield* afs.isFile(filepath)).toBe(false)
      const stat = yield* afs.stat(filepath).pipe(Effect.catch(() => Effect.succeed(undefined)))
      expect(stat).toBeUndefined()
    }),
  )

  it.instance("updates see staged content from disk baseline", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "base.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "line1\nline2\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "line1\nchanged\n")

      expect(yield* afs.readFileString(filepath)).toBe("line1\nchanged\n")
      expect(yield* readDisk(filepath)).toBe("line1\nline2\n")
    }),
  )

  it.instance("stream returns staged content for a file that also exists on disk", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "stream.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "disk\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "staged\n")

      const chunks = yield* Stream.runCollect(afs.stream(filepath))
      expect(new TextDecoder().decode(Buffer.concat(Array.from(chunks)))).toBe("staged\n")
    }),
  )

  it.instance("stream honors offset and bytesToRead on staged content", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "window.txt")
      yield* afs.writeWithDirs(filepath, "0123456789")

      const chunks = yield* Stream.runCollect(afs.stream(filepath, { offset: 2, bytesToRead: 4 }))
      expect(new TextDecoder().decode(Buffer.concat(Array.from(chunks)))).toBe("2345")
    }),
  )

  it.instance("stream fails with NotFound for a deleted overlay path", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "deleted-stream.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "on disk", "utf-8"))
      ReviewOverlay.markDeleted(filepath)

      const error = yield* Stream.runCollect(afs.stream(filepath)).pipe(Effect.flip)
      expect(error.reason._tag).toBe("NotFound")
    }),
  )

  it.instance("open reads staged bytes for a file that only exists in the overlay", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "new-only-staged.txt")
      yield* afs.writeWithDirs(filepath, "brand new")

      const sample = yield* Effect.scoped(
        Effect.gen(function* () {
          const file = yield* afs.open(filepath, { flag: "r" })
          return Option.getOrUndefined(yield* file.readAlloc(5))
        }),
      )
      expect(new TextDecoder().decode(sample)).toBe("brand")
      expect(yield* readDisk(filepath)).toBe("")
    }),
  )

  it.instance("stream and open pass through to disk when review mode is off", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service

      const filepath = path.join(test.directory, "off.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "on disk", "utf-8"))
      ReviewOverlay.stage(filepath, "ignored while disabled")

      const chunks = yield* Stream.runCollect(afs.stream(filepath))
      expect(new TextDecoder().decode(Buffer.concat(Array.from(chunks)))).toBe("on disk")
      const sample = yield* Effect.scoped(
        Effect.gen(function* () {
          const file = yield* afs.open(filepath, { flag: "r" })
          return Option.getOrUndefined(yield* file.readAlloc(7))
        }),
      )
      expect(new TextDecoder().decode(sample)).toBe("on disk")
    }),
  )

  it.instance("drops a staged entry when the disk is changed by someone else", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)
      ReviewOverlay.setActiveSession("sess-1")

      const filepath = path.join(test.directory, "external.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "original\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "staged\n")
      yield* Effect.promise(() => fs.writeFile(filepath, "changed by shell\n", "utf-8"))

      expect(yield* afs.readFileString(filepath)).toBe("changed by shell\n")
      expect(ReviewOverlay.get(filepath)).toBeUndefined()
      expect(ReviewOverlay.drainPendingWrites()).toEqual([])
    }),
  )

  it.instance("keeps serving staged content once the client wrote it to disk", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "applied.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "original\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "staged\n")
      yield* Effect.promise(() => fs.writeFile(filepath, "staged\n", "utf-8"))

      expect(yield* afs.readFileString(filepath)).toBe("staged\n")
      expect(ReviewOverlay.get(filepath)).toBeDefined()
    }),
  )

  it.instance("consecutive edits survive a client that applies each write immediately", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "consecutive.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "c0\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "c1\n")
      yield* Effect.promise(() => fs.writeFile(filepath, "c1\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "c2\n")

      expect(yield* afs.readFileString(filepath)).toBe("c2\n")
    }),
  )

  it.instance("a staged new file stays visible while it is not on disk", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "brand-new.txt")
      yield* afs.writeWithDirs(filepath, "created\n")

      expect(yield* afs.exists(filepath)).toBe(true)
      expect(yield* afs.readFileString(filepath)).toBe("created\n")
    }),
  )

  it.instance("stream serves the disk after an external change", () =>
    Effect.gen(function* () {
      const test = yield* TestInstance
      const afs = yield* FSUtil.Service
      ReviewOverlay.setEnabled(true)

      const filepath = path.join(test.directory, "stream-external.txt")
      yield* Effect.promise(() => fs.writeFile(filepath, "original\n", "utf-8"))
      yield* afs.writeWithDirs(filepath, "staged\n")
      yield* Effect.promise(() => fs.writeFile(filepath, "changed by shell\n", "utf-8"))

      const chunks = yield* Stream.runCollect(afs.stream(filepath))
      expect(new TextDecoder().decode(Buffer.concat(Array.from(chunks)))).toBe("changed by shell\n")
    }),
  )
})
