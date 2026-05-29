# First Setup

Setting up your Arcanum is designed to be easy. Here's how you do it.

## Guided Install

There is a command for installing Arcanum on your machine. However, this assumes
you will only be running one Arcanum on your system.

```bash
curl https://install.arcmaximizer.com/arcanum | bash
```

You will then have to name your Arcanum. Naming it is not *strictly* required,
but as a result your node will not be able to access Arcnet. Go with something
new and short - Arcanum is a small network, claim the short names before anyone
else!

```
Arcanum installer ===

Do you already have an identity? No
Choose a name: arc

Keypair registered with Arcanum Identity Service
Welcome to Arcanum, ^arc! Start your node using arcanum start.
```

If you have an existing node, you may make your new node a child, analogous to
moons in Urbit. This node will be an independent node such as `^arc.sol` but
controlled by your main node like `^arc`.

```
Arcanum installer ===

Do you already have an identity? No
Choose a name: arc.sol

Looks like you're making a child node! This will tether your node's identity to ^arc.
This is meant for IoT devices, scripts, bots or testing.

Press enter to proceed.

^arc has been notified. Waiting for approval...

Keypair registered with ^arc and Arcanum Identity Service
Welcome to Arcanum, ^arc.sol! Start your node using arcanum start.
```

Start up your node:

```
~$ arcanum

Arcanum 0.1

Your Arcanum, ^arc, is up and running.
Access it through the following:
  localhost:6202

Press ^C to leave the interactive shell.
Press ^D to shut down your Arcanum.
(^C will leave your Arcanum running!)
```

## Installing apps

You can install an app from the official app registry, or sideload it from your
local machine.

```
~$ arcanum

Arcanum 0.1

Your Arcanum, ^arc, is up and running.

Press ^C to leave the interactive shell.
Press ^D to shut down your Arcanum.
(^C will leave your Arcanum running!)

^arc: install arc/forum
 Downloaded ^arc/forum:1.0.1 from the store
 Installed ^arc/forum:1.0.1
 Bound port ^arc:forum

 This is primarily a web-based app.
 Make sure to set up a domain like forum.example.org!
 You can do this using the command:
  > sys/http-server add arc/forum forum.example.org

^arc: sideload forum.zip
 Installed forum.zip as ^arc/forum:1.0.3
 Over-the-air updates for ^arc/forum disabled
```

## Setting up a reverse proxy

Arcanum's HTTP server listens on port 6202 by default. (Why 6202? It was chosen
for a good reason, but it is lost to time.) You can point a reverse proxy such
as Caddy or nginx to it.

You can configure supported URIs in `arcanum shell`.

Processes may also configure their own URIs at runtime if they want to be
exposed to the web.

```
~$ arcanum

Arcanum 0.1

Your Arcanum, ^arc, is up and running.

Press ^C to leave the interactive shell.
Press ^D to shut down your Arcanum.
(^C will leave your Arcanum running!)

^arc: sys/http-server add arc/blog blog.arcmaximizer.com
 New URI for ^arc/blog manually registered (1 URI)

^arc: sys/http-server list-uris arc/blog
 ^arc/blog is accessible by the following URI(s):
 - blog.arcmaximizer.com
```

## Updating apps

Apps must be updated every once in a while. Your Arcanum should download and
reinstall apps in the background. Updating an app is a zero-downtime process.

To update an app installed via sideloading, simply use the `sideload` command
again.

```
~$ arcanum

Arcanum 0.1

Your Arcanum, ^arc, is up and running.

Press ^C to leave the interactive shell.
Press ^D to shut down your Arcanum.
(^C will leave your Arcanum running!)

^arc: update arc/blog
 Downloaded ^arc/blog:0.2 from the store
 Installed ^arc/blog:0.2 (previous: 0.1)
 Partially unloaded previous executor (2 event(s) pending)
```
