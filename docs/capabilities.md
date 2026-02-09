# Capabilities

Capabilities, also sometimes called caps, are the way Arcanum apps can allow
each other to access certain restricted functions. Apps can be granted
capabilities through one of three ways:

1. During installation in `arcanum.yaml`
2. Message passing from the app
3. Requesting it from `sys/settings`

Capabilities are not just strings: they are objects with a name and additional
optional metadata.

Capabilities also aren't fully local either. An app can request a capability
from an app running on a remote node, and the runtime will store this capability
information for future use.

By default, all capabilities specific to the receiver are passed when sending a
message. For example, if you are to send a message to `sys/http` while having
the capability `sys/http:send`, it will automatically send that capability along
with your message.

If an Arcanum receives a message with an invalid set of capabilities, it will
not reach the app at all. Instead, it will be responded to with a
`CapabilityError`.

## Usage

For example, let's say you're hosting a discussion forum for your crochet
community. When subscribing to your forum, your friends' nodes can grant your
forum the capability `arc/forum:update` and the metadata `arcs-crochet-forum`.

Later on, when a new post or comment comes out, you'll be able to push out
updates to all your subscribers. The subscriber will check your request's
capabilities before then accepting it.
