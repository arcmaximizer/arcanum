# Optimization & Customizations

## Scheduling

You can control how your app is scheduled. This is important when dealing with
multiple requests hitting state at once.

By default, apps use the OCC scheduling mode[^1] which should be a good fit for
most. However, if your app modifies or reads the same state in every single
call, it'll end up causing events to be re-executed with sufficiently high
throughput.

For example, let's say your app receives hundreds of events to increment a
counter. By default, all of them will run concurrently[^2].

The available modes are

- `occ`: Optimistic Concurrency Control (default)
- `serial`: Process only one request at a time

## Blocking code

Don't do this. Instead, please create a new process for any blocking computations.

## Footnotes

[^1]: Optimistic Concurrency Control

[^2]: Subject to the JavaScript event loop. If you block it, you will degrade
    performance.
