# App Config Reference

An example config with everything populated and commented:

```yaml
# arcanum.yaml

# The internal ID of the app.
id: local/my-app

# Semantic versioning required.
version: 0.0.0

# Human-readable content
name: My App
desc: My awesome app

# The file that contains the exported handlers.
entrypoint: main.ts

# Aliases are used to receive messages using the ID of another app.
#
# Can be used for switching implementations. For example, if you run a forum
# and want to switch to a new forum app that is compatible at the API level,
# you can avoid having to reconfigure capabilities or recipients.
#
# If the app is already installed, messages to it will not be forwarded.
aliases:
  - arcmaximizer/my-app

# Grants the following capabilities from other local apps onto this app.
#
# If the app is not installed yet, it will grant the capability automatically
# once that app where the capability is from is installed.
#
# Comma syntax is used to delineate multiple capabilities from the same app.
# e.g. sys/arcnet:send,receive -> sys:arcnet/send + sys:arcnet/receive
#
# You cannot provide metadata in this field. Request the capability from the
# settings app at sys/settings in order to do so.
capabilities:
  - sys/http:receive
  - sys/arcnet:send,receive

# Requests the following domains from the HTTP module. Requires the app to
# have the capability sys/http:receive, otherwise it will no-op. When the
# capability is granted, it will automatically begin directing traffic.
#
# The value @* is replaced by the node's wildcard domain at runtime.
# For example, my-app--mynode.tryarcanum.org
#
# These domain(s) may be changed at runtime with a message to sys/http if the
# app has the capability sys/http:rename.
domains:
  - my-app@*
```
