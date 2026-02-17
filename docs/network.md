# Networking

Arcanum nodes communicate with each other over the Arcnet. From the perspective
of your app, a local message is not natively distinguished from a remote
message: you have to check the sender.

When passed over the wire, a message is serialized as JSON. However, your app
can safely receive and send JavaScript objects as long as they are serializable.

Arcnet is currently implemented as an entirely centralized networking service
operated by the developers through Cloudflare Workers. This is planned to change
in the future.

## Sending a message

Inside your app's event handlers, you will be passed a `ctx` object which
provides various different functions for storing state, creating timers, but
most importantly, networking.

To send a message to a different Arcanum node, use:

```
await ctx.send(receiver, message);
```

The receiver is of the format `dev/app@remote-node`. More formally, it must
satisfy the following regex:
`^([a-z0-9](?:-?[a-z0-9])*)\/([a-z0-9](?:-?[a-z0-9])*)@([a-z0-9](?:-?[a-z0-9])*)$`

If you want to message an app on your own Arcanum, simply pass the node name
`local`. It is a reserved name that always maps to the local node.

The message may be any `Serializable`. It will automatically be serialized into
JSON and then deserialized on the other end.

## Receiving messages

Your app can natively receive data which then appears as `req`.

```
async function onArcnet(req, env, ctx) {
  return "Hello world!";
}
```
