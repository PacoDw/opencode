import type { PromptResponse } from "@agentclientprotocol/sdk"
import { describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import path from "node:path"
import { createAcpFixture, expectOk, initialize, newSession, toolCallStream } from "./subprocess"

// The title request offers no tools; after the write, the next request carries its result.
function writingModel(request: unknown) {
  const body = JSON.stringify(request)
  if (!body.includes('"name":"write"') || body.includes('"role":"tool"')) return "done"
  return toolCallStream("call_write", "write", { path: "hello.txt", content: "hello\n" })
}

const exists = (file: string) =>
  fs.access(file).then(
    () => true,
    () => false,
  )

// The real CLI, its private server, and the write tool together: the edit must reach the client, not the disk.
describe("acp client file writes subprocess", () => {
  test("a tool write in the session's directory goes through fs/write_text_file", async () => {
    await using fixture = await createAcpFixture({ respond: writingModel })
    const target = path.join(fixture.home, "hello.txt")
    const acp = fixture.spawn()
    await initialize(acp, { fs: { writeTextFile: true } })
    const session = await newSession(acp, fixture.home)

    const prompt = acp.request<PromptResponse>("session/prompt", {
      sessionId: session.sessionId,
      prompt: [{ type: "text", text: "Create hello.txt" }],
    })
    const write = await acp.clientRequest("fs/write_text_file")

    expect(write.params).toEqual({ sessionId: session.sessionId, path: target, content: "hello\n" })
    // opencode handed the write to the client instead of writing the file itself.
    expect(await exists(target)).toBe(false)
    await Bun.write(target, "hello\n")
    await acp.respond(write.id, {})

    expect(expectOk(await prompt).stopReason).toBe("end_turn")
    expect(await fs.readFile(target, "utf8")).toBe("hello\n")
    expect(JSON.stringify(fixture.llm.requests.at(-1))).toContain("Created file successfully: hello.txt")
  }, 60_000)
})
