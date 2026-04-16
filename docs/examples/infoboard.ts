export const processes = {
  infoboard: {
    id: "infoboard",
    handler: async (ctx, msg) => {
      const kv = ctx.kv;
      const ready = await kv.get("ready");

      switch (msg.type) {
        case "get": {
          return ready
            ? {
              name: await kv.get("name"),
              bio: await kv.get("bio"),
              links: await kv.get("links"),
            }
            : { name: "N/A", bio: "Not set up yet." };
        }
        case "setBio": {
          await kv.set("bio", msg.bio);
          break;
        }
        case "setName": {
          await kv.set("name", msg.name);
          break;
        }
        case "setLinks": {
          await kv.set("links", msg.links);
          break;
        }
      }
    },
  },
};
