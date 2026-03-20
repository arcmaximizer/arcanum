export default async function (
  from: string,
  req: { key: string; value: unknown },
  ctx: any,
) {
  await ctx.set(req.key, req.value);
  return { written: req.key };
}
