![Arcanum](./header.png)

Deterministic, functional, sovereign. Arcanum is personal server software
allowing you to build applications in TypeScript that benefit from
version-controlled state and upgrades, peer-to-peer networking and more as a
pure function of the event log.

## Installation

It is recommended to run Arcanum using Docker.

```
docker run -v storage:/app/data arcmaximizer/arcanum
```

On first boot, the Arcanum node may be provided with a `firstboot.yml` file
mounted in the Arcanum data directory. This will be used to preinstall apps, set
up identity and other aspects.

An example `firstboot.yml` is provided below.

```yaml
node_name: Arcanum Release Computer
node_identity: ./node.key

user_name: arcmaximizer

apps:
  - id: arcmaximizer/webserver
    options:
      domain: arcmaximizer.onarcanum.xyz
```