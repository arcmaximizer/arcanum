export const processes = {
  board: {
    id: "board",
    handler: async (ctx, msg) => {
      const { type, ...args } = msg;

      switch (type) {
        case "addPost":
          return addPost(args.content);
        case "addComment":
          return addComment(args.target, args.content);
        case "getPosts":
          return getPosts(args.count, args.cursor);
        case "getPost":
          return getPost(args.target);
      }
    },
  },
};

async function addPost(content: string) {
  return ctx.lists.posts.append({
    from: ctx.id,
    content,
    time: Date.now(),
  });
}

async function addComment(target: number, content: string) {
  return ctx.lists.comments.append({
    target,
    from: ctx.id,
    content,
    time: Date.now(),
  });
}

async function getPosts(count: number, cursor?: number) {
  if (count > 100) throw new Error("Max count: 100");

  return ctx.lists.posts.find({
    condition: `id < ${cursor}`,
    maxResults: count,
    sort: "desc",
    extras: { appendId: true },
  });
}

async function getPost(target: number) {
  const post = ctx.lists.posts.get(target);
  const comments = ctx.lists.comments.find({
    condition: `target = ${target}`,
    extras: { appendId: true },
  });
  return { post, comments };
}
