// This app stores a global counter.

export default async function (from, req, ctx) {
  const value = await ctx.get("value");

  switch (req.command) {
    case "raise": {
      await ctx.set("value", value + 1);
      return value + 1;
    }
    case "lower": {
      await ctx.set("value", value - 1);
      return value - 1;
    }
    default: {
      throw "Invalid command";
    }
  }
}
