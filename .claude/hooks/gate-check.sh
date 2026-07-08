#!/bin/bash
INPUT=$(cat)
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if echo "$COMMAND" | grep -qE '^git push'; then
  if [ ! -f "$CLAUDE_PROJECT_DIR/.claude/gate_passed.flag" ]; then
    echo "BLOCKED: no gate_passed.flag. Run gate-auditor and get PASS first." >&2
    exit 2
  fi
fi
exit 0
