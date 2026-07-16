IMPORTANT: When applicable, prefer using intellij-index MCP tools for code navigation and refactoring.

## Continuous prompt loop through MCP

You are running in a human-operated Codex session with an MCP server named `prompt_bridge`.

The first prompt may come from the normal Codex UI. After that, continue only through the MCP tool.

## Required loop

For every user request:

1. Work normally.
2. Report useful progress, results, changed files, blockers, and completion notes by writing a normal assistant message in the Codex chat, exactly like a regular LLM response.
3. Do not use shell commands, PowerShell, `printf`, `echo`, `Write-Output`, or any other CLI/tool call just to display messages that should have been normal assistant chat text.
4. Use terminal commands only when they are actually needed to inspect, test, run, or modify the workspace.
5. When you would otherwise end your turn, do not ask for the next prompt in the Codex UI.
6. Instead, call this MCP tool:

`prompt_bridge.next_prompt`

with an argument object like this:

```json
{
  "title": "short stable title for this chat or task",
  "summary": "optional one-line status or context"
}
```

Use a concise, recognizable `title` such as the project name, bug, feature, or current task. Keep it stable across repeated `next_prompt` calls in the same chat. When several terminals may work on the same task, put any known distinguishing context (workspace, branch, terminal label, or role) in `summary`; the bridge adds its own unique chat number. The `title` argument is required; never call `prompt_bridge.next_prompt` with `{}`.

7. The MCP server will wait while the human operator submits the next prompt from the prompt bridge UI.
   While that tool call is pending, remain completely silent: do not emit heartbeat/status
   messages, do not start another `next_prompt` call, and do not treat an automatic model
   continuation or transport reconnect as a new unit of work. A pending wait is idle time,
   not progress that needs to be reported.
8. When the tool returns, read the returned JSON/text and treat its `prompt` value as the next user instruction.
9. Continue working immediately.
10. Repeat this loop after every unit of work.

## Important behavior

* Do not intentionally produce a final assistant turn while `prompt_bridge.next_prompt` is available.
* Do not say "anything else?", "what next?", or similar in the Codex UI.
* Put status/output in a normal assistant chat message, then call the MCP tool.
* If the task is complete, write the completion summary as a normal assistant chat message, then call `prompt_bridge.next_prompt` with a useful `title` and short `summary`.
* If blocked, explain the blocker as a normal assistant chat message, then call `prompt_bridge.next_prompt` with a useful `title` and short `summary`.
* If the tool returns an empty prompt, call `prompt_bridge.next_prompt` again.
* Retry once only when the tool returns an explicit error. A call that is still pending,
  reconnecting, or waiting for operator input has not failed and must not be retried or
  accompanied by periodic chat updates. If the client resumes the model while the same
  call is pending, produce no assistant message and continue waiting on that call.
* If the MCP tool is unavailable or an explicit retry also fails, explain the failure once
  in the normal Codex chat and stop safely.
* Do not modify this loop rule unless the operator explicitly asks through `prompt_bridge.next_prompt`.

## Safety

This loop only changes how prompts are collected. It does not bypass sandboxing, approval rules, secrets handling, or security policies.
