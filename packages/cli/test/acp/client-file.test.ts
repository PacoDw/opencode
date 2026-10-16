import { describe, expect, test } from "bun:test"
import path from "node:path"
import { RequestError } from "@agentclientprotocol/sdk"
import { clientFileWriteRequested, makeSession, startSession, startWire } from "./wire-fixture"

const workspace = path.resolve("/workspace")
const shared = path.resolve("/shared/lib")
const extra = path.resolve("/extra")

describe("acp client file writes over the wire", () => {
  test("attaches a session's directories only when the client can write files", async () => {
    await using writer = await startWire()
    await writer.initialize({ writeTextFile: true })
    const created = await writer.request("session/new", {
      cwd: workspace,
      additionalDirectories: [shared],
      mcpServers: [],
    })

    expect(writer.server.clientFiles.attached).toEqual([{ sessionID: created.sessionId, roots: [workspace, shared] }])

    await using reader = await startWire()
    await reader.initialize()
    await reader.newSession(workspace)

    expect(reader.server.clientFiles.attached).toEqual([])
  })

  test("re-attaches with the directories a resume requests, not the ones stored before", async () => {
    await using acp = await startWire()
    acp.server.sessions.set("ses_saved", {
      ...makeSession("ses_saved", { cwd: workspace }),
      metadata: { "opencode.acp.additionalDirectories": [shared] },
    })
    await acp.initialize({ writeTextFile: true })

    await acp.request("session/resume", { sessionId: "ses_saved", cwd: workspace, additionalDirectories: [extra] })

    expect(acp.server.clientFiles.attached).toEqual([{ sessionID: "ses_saved", roots: [workspace, extra] }])
  })

  test("writes each requested file through the client and replies", async () => {
    await using acp = await startSession({ capabilities: { writeTextFile: true } })
    const target = path.join(workspace, "src", "index.ts")

    acp.server.send(clientFileWriteRequested("ses_other", "cfw_other", { path: target, content: "ignored" }))
    acp.server.send(clientFileWriteRequested(acp.sessionId, "cfw_1", { path: target, content: "after\n" }))
    await acp.until(() => acp.server.clientFiles.replies.length > 0, "client file reply")

    expect(acp.fileWrites).toMatchObject([{ sessionId: acp.sessionId, path: target, content: "after\n" }])
    expect(acp.server.clientFiles.replies).toEqual([{ sessionID: acp.sessionId, requestID: "cfw_1" }])
  })

  test("reports the client's failure to the server", async () => {
    await using acp = await startSession({
      capabilities: { writeTextFile: true },
      writeTextFile: () => {
        throw RequestError.invalidParams({ path: "/elsewhere" }, "invalid path")
      },
    })

    acp.server.send(
      clientFileWriteRequested(acp.sessionId, "cfw_rejected", { path: path.join(workspace, "a.ts"), content: "x" }),
    )
    await acp.until(() => acp.server.clientFiles.replies.length > 0, "client file reply")

    expect(acp.server.clientFiles.replies).toEqual([
      { sessionID: acp.sessionId, requestID: "cfw_rejected", error: expect.stringContaining("invalid path") },
    ])
  })

  test("detaches a session when the client closes it", async () => {
    await using acp = await startSession({ capabilities: { writeTextFile: true } })

    await acp.request("session/close", { sessionId: acp.sessionId })

    expect(acp.server.clientFiles.detached).toEqual([acp.sessionId])
  })
})
