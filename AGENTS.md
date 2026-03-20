# Agent Instructions

## Testing

- Use `deno test` - NOT `npm test`
- Always use `--allow-all` flag: `deno test --allow-all`
- Do NOT run `npm install`, `npm test`, or any npm commands

## Type Checking

- Use `deno check <file>` for type checking
- Example: `deno check svc/events.ts`

## Running Code

- Use `deno run` or `deno task` commands
- Check `deno.json` for available tasks

## Committing

- Use Conventional Commits
- Follow commit best practice: not too granular, not too big
- Format all code before committing using `deno fmt`
