// Infoboard: a program to show off your node and who you are
import { ctx, kv, Process, process } from "@arcanum/std";

export default async function entrypoint(req) {
  const info: Info = ctx.getProcess("info");

  if (ctx.from == "sys/arcnet") {
    return await info.getInfo();
  } else {
    switch (req.data.type) {
      case "get": {
        return await info.getInfo();
      }
      case "setBio": {
        return await info.setBio(req.bio);
      }
      case "setName": {
        return await info.setName(req.name);
      }
      case "setLinks": {
        return await info.setLinks(req.links);
      }
      default: {
        throw new Error("Invalid request");
      }
    }
  }
}

@process("info")
class Info implements Process {
  async getInfo() {
    const ready = await kv.get("ready");

    if (!ready) {
      return {
        name: "N/A",
        bio:
          "This node's infoboard hasn't been set up yet. Tell the node owner to set it up!",
      };
    }
    const bio = await kv.get("bio");
    const name = await kv.get("name");
    const links = await kv.get("links");

    return { name, bio, links };
  }
  async setBio(bio: string) {
    await kv.set("bio", bio);
  }
  async setLinks(links: { text: string; url: string }) {
    await kv.set("links", links);
  }
  async setName(name: string) {
    await kv.set("name", name);
  }
}
