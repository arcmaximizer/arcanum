# Agent Instructions

## Committing

- Use Conventional Commits
- Follow commit best practice: not too granular, not too big
- Use `cargo fmt` to format all code

## Other Instructions

- Please don't use `tail` at the end of a command that you expect to have less than ~100 lines. e.g. `cargo test 2>&1 | tail -5` is a nono
