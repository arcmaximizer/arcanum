@CONTRIBUTING.md

# Agent Instructions

## Committing

- Use Conventional Commits
- Follow commit best practice: not too granular, not too big

## Before Pushing

Always run these and fix any issues before pushing:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

## Other Instructions

- Markdown should have 80 character lines, with exception of things that can't
  be cut off (e.g. fake shell output) or aren't for humans (e.g. AGENTS)
- Please don't use `tail` at the end of a command that you expect to have less
  than ~100 lines
- Please don't use subagents
