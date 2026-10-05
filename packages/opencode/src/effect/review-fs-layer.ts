import { Effect, FileSystem, Layer, Option, Stream } from "effect"
import * as PlatformError from "effect/PlatformError"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import { filesystem } from "@opencode-ai/core/effect/app-node-platform"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { ReviewOverlay } from "@opencode-ai/core/review-overlay"

// FSUtil layer for ACP review mode. It wraps the real FSUtil: writes are sent to
// the ReviewOverlay (staged in memory) and reads return staged content, so the
// agent sees its own pending edits without touching disk. When review mode is
// off, every call passes straight through to the real FSUtil.

const overlayNotFound = (method: string, path: string) =>
  PlatformError.systemError({
    _tag: "NotFound",
    module: "FileSystem",
    method,
    pathOrDescriptor: path,
    syscall: method,
  })

function overlayFileStat(content: string): FileSystem.File.Info {
  const now = Option.some(new Date())
  return {
    type: "File",
    mtime: now,
    atime: now,
    birthtime: now,
    dev: 0,
    rdev: Option.some(0),
    ino: Option.none(),
    mode: 0o644,
    nlink: Option.some(1),
    uid: Option.none(),
    gid: Option.none(),
    size: FileSystem.Size(new TextEncoder().encode(content).length),
    blksize: Option.none(),
    blocks: Option.none(),
  }
}

const overlayReadOnly = (method: string, path: string) =>
  PlatformError.systemError({
    _tag: "PermissionDenied",
    module: "FileSystem",
    method,
    pathOrDescriptor: path,
    syscall: method,
  })

// Read-only handle over staged content. The read tool samples files through
// open() and readAlloc(); a file that only exists in the overlay has no disk
// file to open.
function overlayFile(path: string, content: string): FileSystem.File {
  const bytes = new TextEncoder().encode(content)
  let position = 0
  return {
    [FileSystem.FileTypeId]: FileSystem.FileTypeId,
    fd: FileSystem.FileDescriptor(-1),
    stat: Effect.succeed(overlayFileStat(content)),
    seek: (offset, from) =>
      Effect.sync(() => {
        position = from === "start" ? Number(offset) : position + Number(offset)
      }),
    sync: Effect.void,
    read: (buffer) =>
      Effect.sync(() => {
        const chunk = bytes.subarray(position, position + buffer.length)
        buffer.set(chunk)
        position += chunk.length
        return FileSystem.Size(chunk.length)
      }),
    readAlloc: (size) =>
      Effect.sync(() => {
        if (position >= bytes.length) return Option.none()
        const chunk = bytes.slice(position, position + Number(size))
        position += chunk.length
        return Option.some(chunk)
      }),
    truncate: () => Effect.fail(overlayReadOnly("truncate", path)),
    write: () => Effect.fail(overlayReadOnly("write", path)),
    writeAll: () => Effect.fail(overlayReadOnly("writeAll", path)),
  }
}

// Scoped on purpose: only the tools that read and write user files in review
// mode (read, edit, write, apply_patch) receive this wrapper, from ToolRegistry.
// Replacing FSUtil for the whole server graph would also stage internal writes
// (storage, config, caches) and ship them to the client as agent edits.
export function wrap(fs: FSUtil.Interface) {
  // Resolve the staged entry for a path, dropping it when the disk changed behind
  // the overlay (shell, user, another process). Serving or sending the stale copy
  // would silently undo that change.
  const entryFor = Effect.fnUntraced(function* (path: string) {
    const entry = ReviewOverlay.get(path)
    if (!entry) return entry
    if (!(yield* changedOnDisk(path, entry))) return entry
    ReviewOverlay.discard(path)
    return undefined
  })

  // Deletes reach disk before they are marked (ACP has no deleteFile), so a
  // deleted path that exists again was restored by someone else. Staged content
  // is stale once the disk holds a version the overlay neither produced nor
  // started from.
  const changedOnDisk = Effect.fnUntraced(function* (path: string, entry: ReviewOverlay.Entry) {
    if ("deleted" in entry) return yield* fs.existsSafe(path)
    const disk = yield* fs.readFileStringSafe(path).pipe(Effect.catch(() => Effect.succeed(entry.content)))
    return ReviewOverlay.isStale(path, disk)
  })

  const readFileString = Effect.fn("ReviewFs.readFileString")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.readFileString(path)
    const entry = yield* entryFor(path)
    if (entry) {
      if ("deleted" in entry) return yield* Effect.fail(overlayNotFound("readFileString", path))
      return entry.content
    }
    return yield* fs.readFileString(path)
  })

  const readFile = Effect.fn("ReviewFs.readFile")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.readFile(path)
    const entry = yield* entryFor(path)
    if (entry) {
      if ("deleted" in entry) return yield* Effect.fail(overlayNotFound("readFile", path))
      return new TextEncoder().encode(entry.content)
    }
    return yield* fs.readFile(path)
  })

  const readFileStringSafe = Effect.fn("ReviewFs.readFileStringSafe")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.readFileStringSafe(path)
    const entry = yield* entryFor(path)
    if (entry) {
      if ("deleted" in entry) return undefined
      return entry.content
    }
    return yield* fs.readFileStringSafe(path)
  })

  const exists = Effect.fn("ReviewFs.exists")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.exists(path)
    const entry = yield* entryFor(path)
    if (entry) return !("deleted" in entry)
    return yield* fs.exists(path)
  })

  const existsSafe = Effect.fn("ReviewFs.existsSafe")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.existsSafe(path)
    const entry = yield* entryFor(path)
    if (entry) return !("deleted" in entry)
    return yield* fs.existsSafe(path)
  })

  const stat = Effect.fn("ReviewFs.stat")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.stat(path)
    const entry = yield* entryFor(path)
    if (entry) {
      if ("deleted" in entry) return yield* Effect.fail(overlayNotFound("stat", path))
      const disk = yield* fs.stat(path).pipe(Effect.catch(() => Effect.succeed(undefined)))
      if (disk?.type === "File") {
        return { ...disk, size: FileSystem.Size(new TextEncoder().encode(entry.content).length) }
      }
      if (disk) return disk
      return overlayFileStat(entry.content)
    }
    return yield* fs.stat(path)
  })

  const isFile = Effect.fn("ReviewFs.isFile")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.isFile(path)
    const entry = yield* entryFor(path)
    if (entry) return !("deleted" in entry)
    return yield* fs.isFile(path)
  })

  const isDir = Effect.fn("ReviewFs.isDir")(function* (path: string) {
    if (!ReviewOverlay.isEnabled()) return yield* fs.isDir(path)
    const entry = yield* entryFor(path)
    if (entry) return false
    return yield* fs.isDir(path)
  })

  const writeWithDirs = Effect.fn("ReviewFs.writeWithDirs")(function* (
    path: string,
    content: string | Uint8Array,
    mode?: number,
  ) {
    // Only text writes can be staged and shown in the review UI. Binary writes
    // (and any write while review mode is off) go straight to disk.
    if (!ReviewOverlay.isEnabled() || typeof content !== "string") {
      return yield* fs.writeWithDirs(path, content, mode)
    }
    // A new entry, or one that replaces a delete, starts from the disk as it is now.
    const entry = yield* entryFor(path)
    if (!entry || "deleted" in entry) ReviewOverlay.trackBase(path, yield* fs.readFileStringSafe(path))
    ReviewOverlay.stage(path, content)
  })

  // The read tool streams text lines and samples bytes through these two, so they
  // must see staged content too or the agent re-reads its file without its edits.
  const stream: FSUtil.Interface["stream"] = (path, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        if (!ReviewOverlay.isEnabled()) return fs.stream(path, options)
        const entry = yield* entryFor(path)
        if (!entry) return fs.stream(path, options)
        if ("deleted" in entry) return Stream.fail(overlayNotFound("stream", path))
        const bytes = new TextEncoder().encode(entry.content)
        const start = Number(options?.offset ?? 0)
        const end = options?.bytesToRead === undefined ? bytes.length : start + Number(options.bytesToRead)
        return Stream.make(bytes.slice(start, end))
      }),
    )

  const open: FSUtil.Interface["open"] = (path, options) =>
    Effect.gen(function* () {
      if (!ReviewOverlay.isEnabled()) return yield* fs.open(path, options)
      const entry = yield* entryFor(path)
      if (!entry) return yield* fs.open(path, options)
      if ("deleted" in entry) return yield* Effect.fail(overlayNotFound("open", path))
      // Only read handles are served from the overlay; writes are staged through writeWithDirs.
      if (options?.flag !== undefined && options.flag !== "r") return yield* fs.open(path, options)
      return overlayFile(path, entry.content)
    })

  return FSUtil.Service.of({
    ...fs,
    readFileString,
    readFile,
    readFileStringSafe,
    exists,
    existsSafe,
    stat,
    isFile,
    isDir,
    writeWithDirs,
    stream,
    open,
  })
}

const reviewLayer = Layer.effect(
  FSUtil.Service,
  Effect.gen(function* () {
    return wrap(yield* FSUtil.Service)
  }),
)

export const node = makeGlobalNode({
  service: FSUtil.Service,
  layer: reviewLayer.pipe(Layer.provide(LayerNode.compile(FSUtil.node))),
  deps: [filesystem],
})

export const defaultLayer = LayerNode.compile(node)

export const layer = defaultLayer

export * as ReviewFs from "./review-fs-layer"
