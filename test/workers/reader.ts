export default async function (from: string, req: { key: string }, ctx: any) {
  const value = await ctx.get(req.key);
  return { key: req.key, value };
}
