# Contributing

## Workflow

The project is currently in extreme pre-alpha state: maintainers may commit
directly to master at any point.

If you're an outside contributor, create a feature branch on your own fork of
the repo such as `git checkout -b feat/add-hamburgers`, then make changes to
that branch. Once you're done, submit the contribution as a PR and await
maintainer approval.

Commits should be named using Conventional Commits. Before pushing, run:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

Linting, formatting, and tests are also enforced by CI.

## Vibe coding

Vibe-coded contributions will be held to the same standard as human-generated
contributions. An AGENTS is provided.

## CLAs and Legal

The project is currently licensed under the GNU General Public License 3.0, but
we want to be able to offer some users more permissive licenses in the future.
As such, you'll have to sign a CLA to grant us a perpetual license to sublicense
your contributions later on.

The CLA bot will do this for you on your PR. You can also view the CLA at [CLA.md](./CLA.md]).

## Questions?

Create a GitHub issue or discussion!
