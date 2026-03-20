export default async function (
  from: string,
  req: { target: string; input: unknown },
  ctx: any,
) {
  const result = await ctx.call(req.target, req.input);
  return { called: req.target, result };
}
