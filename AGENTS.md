@CONTRIBUTING.md

# Agent Instructions

## Committing

- Use Conventional Commits
- Follow commit best practice: not too granular, not too big
- Use `cargo fmt` to format all code, always lint

## Other Instructions

- Please don't use `tail` at the end of a command that you expect to have less than ~100 lines. e.g. `cargo test 2>&1 | tail -5` is a nono
- Please don't use subagents
- Markdown should have 80 character lines, with exception of things that can't be cut off (e.g. fake shell output) or aren't for humans (e.g. AGENTS)
