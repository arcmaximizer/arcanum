// Arcchan: a publicly available message board over Arcnet
// This is demo software!
import { ctx, lists, Process, process } from "@arcanum/std";

export default async function entrypoint(req) {
  const proc = ctx.from;
  if (proc == "sys/arcnet") {
    return await handle(req);
  } else {
    const data = { from: ctx.node, ...req };
    return await handle(data);
  }
}

async function handle(req) {
  const { from, data } = req;
  if (!data.board) throw new Error("No board");

  // Processes are cheap
  const board: Board = ctx.getProcess("board", data.board);

  switch (data.type) {
    case "post": {
      return await board.post(from, data.content);
    }
    case "comment": {
      return await board.comment(from, data.target, data.content);
    }
    case "getPosts": {
      return await board.getPosts(data.count, data.cursor);
    }
    case "getPost": {
      return await board.getPost(data.target);
    }
    default: {
      // We have no idea what's going on, throw
      throw new Error("Invalid request");
    }
  }
}
@process("board")
class Board implements Process {
  async post(from: string, content: string) {
    const id = await lists.posts.append({
      from,
      content,
      time: Date.now(),
    });
    return id;
  }
  async comment(from: string, target: number, content: string) {
    const id = await lists.comments.append({
      target,
      from,
      content,
      time: Date.now(),
    });
    return id;
  }
  async getPosts(count: number, cursor?: number) {
    if (count > 100) throw new Error("Max count: 100");

    // Find a list of posts
    const posts = await lists.posts.find({
      condition: `id < ${cursor}`,
      maxResults: count,
      sort: "desc",
      extras: {
        // Returns { id: number, ... }
        // This will throw an error if any element already has the "id" property
        appendId: true,
      },
    });

    return posts;
  }

  async getPost(target: number) {
    const post = await lists.posts.get(target);
    const comments = await lists.comments.find({
      condition: `target = ${target}`,
      extras: {
        appendId: true,
      },
    });

    return {
      post,
      comments,
    };
  }
}
