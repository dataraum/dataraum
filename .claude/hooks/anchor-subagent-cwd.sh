#!/bin/bash
# PreToolUse Bash hook — anchor subagent bash commands to the active worktree.
#
# Why: when /take opens a lane in a worktree, spawned subagents (reviewers,
# Explore, etc.) may inherit a $CLAUDE_PROJECT_DIR that resolves to the main
# repo root — so their git/pytest/grep run against main's tree, not the
# lane's branch. The cd-in-prompt workaround is non-deterministic
# (the Bash tool's own docs discourage cd). This hook makes it deterministic
# by rewriting the bash command at intercept time.
#
# How: PreToolUse stdin is JSON. The `agent_id` field is present ONLY for
# subagent calls — that's the signal. /take writes the worktree's absolute
# path into $CLAUDE_PROJECT_DIR/.claude/.worktree-anchor on lane open and
# deletes it on lane close. This hook reads the anchor and prepends
# `cd <path> && ` to the subagent's command via the documented
# `hookSpecificOutput.updatedInput.command` channel.
#
# Pass-through (no modification) when:
#   - jq is not installed
#   - the call is from the orchestrator (no `agent_id`)
#   - no anchor file exists (no active lane)
#   - the anchor path doesn't resolve to a real directory
#   - the command already cd's into the anchor path (idempotency)
#
# Critical: this hook deliberately fails OPEN, not closed. Any nonzero exit
# from a PreToolUse hook is treated by Claude Code as a blocking veto, which
# would silently break every subsequent Bash call in the session. We do NOT
# `set -e`; every external command that could fail is explicitly guarded with
# `|| exit 0` so the pass-through path is the default on any error.

set -uo pipefail

input=$(cat)

if ! command -v jq >/dev/null 2>&1; then
    exit 0
fi

agent_id=$(printf '%s' "$input" | jq -r '.agent_id // empty' 2>/dev/null) || exit 0
if [ -z "$agent_id" ]; then
    # Orchestrator call — no modification.
    exit 0
fi

anchor_file="${CLAUDE_PROJECT_DIR:-.}/.claude/.worktree-anchor"
if [ ! -f "$anchor_file" ]; then
    # No active lane.
    exit 0
fi

# `read -r` preserves embedded spaces in the path (unlike `tr -d '[:space:]'`,
# which would silently mangle worktrees under directories like `/Users/foo/my project/`).
IFS= read -r worktree_path < "$anchor_file" || exit 0
if [ -z "$worktree_path" ] || [ ! -d "$worktree_path" ]; then
    exit 0
fi

original_command=$(printf '%s' "$input" | jq -r '.tool_input.command // empty' 2>/dev/null) || exit 0
if [ -z "$original_command" ]; then
    exit 0
fi

case "$original_command" in
    "cd $worktree_path"*|"cd \"$worktree_path\""*|"cd '$worktree_path'"*)
        exit 0
        ;;
esac

# Quote the path so worktrees under directories with spaces still cd correctly.
new_command="cd \"$worktree_path\" && $original_command"

jq -n --arg cmd "$new_command" '{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "allow",
    "updatedInput": {"command": $cmd}
  }
}' || exit 0

exit 0
