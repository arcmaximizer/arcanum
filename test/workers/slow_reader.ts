export default async function (
  from: string,
  req: { key: string; delay: number },
  ctx: any,
) {
  await new Promise((r) => setTimeout(r, req.delay));
  const value = await ctx.get(req.key);
  return { key: req.key, value };
}
