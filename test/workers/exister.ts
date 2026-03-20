export default async function (
  from: string,
  req: { key: string },
  ctx: any,
) {
  return { key: req.key, exists: await ctx.exists(req.key) };
}
