export default async function (from: string, req: unknown, ctx: any) {
  // Hang forever — never returns, never touches ctx
  await new Promise(() => {});
}
