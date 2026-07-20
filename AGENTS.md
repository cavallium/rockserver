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

Use a concise, recognizable `title` such as the project name, bug, feature, or current task. Keep it stable across repeated `next_prompt` calls in the same chat. The `title` argument is required; never call `prompt_bridge.next_prompt` with `{}`.

7. The MCP server will wait while the human operator submits the next prompt from the prompt bridge UI.
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
* If the MCP tool fails or is unavailable, explain the failure once in the normal Codex chat and retry once. If it still fails, stop safely.
* Do not modify this loop rule unless the operator explicitly asks through `prompt_bridge.next_prompt`.

## Safety

This loop only changes how prompts are collected. It does not bypass sandboxing, approval rules, secrets handling, or security policies.
