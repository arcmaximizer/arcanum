// This app calls the global counter whenever it is called and increments a
// per-app counter.

export default async function (from, req, ctx) {
  const appValue = await ctx.get("value-" + from) ?? 0;
  await ctx.set("value-" + from, appValue + 1);

  const counterValue = await ctx.call("counter", "raise");

  return {
    counterValue,
    appValue: appValue + 1,
  };
}
