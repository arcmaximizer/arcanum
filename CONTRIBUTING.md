# Contributing

To contribute to Arcanum, you'll need to have Deno installed. Deno isn't as
commonly used as Node, so you'll have to familiarize yourself with it first -
but once you get over those initial hurdles it should be smooth sailing!

## Workflow

The project is currently in extreme pre-alpha state: maintainers may commit
directly to master at any point.

If you're an outside contributor, create a feature branch on your local
repository such as `git checkout -b feat/add-hamburgers`, then make changes to
that branch. Once you're done, submit the contribution as a PR and await
maintainer approval!

Commits should be named using Conventional Commits. Always format commits with
`deno fmt`. This will be enforced by a PR bot.

## Vibe coding

Vibe coding is acceptable as long as you actually double-check the code: it
definitely raises the productivity of many devs, but it often has a negative
impact on code quality unless prompted correctly.

## Never break userspace

Arcanum should never break backwards compatibility with apps built for earlier
versions unless for a very very good reason (e.g. security, bug fixes). This is
because we believe in
[building your software to last a thousand years](./docs/principles.md) - we
need to be as autistic as we can about backwards-compatibility in the same way
that Urbit or ECMAScript do.

## CLAs and Funky Legal Business

The project is currently licensed under the GNU General Public License 3.0, but
we want to be able to offer some users more permissive licenses in the future.
As such, you'll have to sign a CLA to grant us a perpetual license to sublicense
your contributions later on.

The CLA bot will do this for you on your PR. You can also view the CLA at
[the CLA.md file](./CLA.md]).

## Questions?

Create a GitHub issue or discussion and we should try to get back to you
shortly!
