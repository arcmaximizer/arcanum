import { ctx, kv } from "@arcanum/std";

const FORTUNES = [
  "You will write clean code today.",
  "A refactor is in your future.",
  "Someone will appreciate your commit messages.",
  "Your tests will pass on the first run.",
  "Today is a good day for a code review.",
  "Beware of hidden dependencies.",
  "The bug you're looking for is one line away.",
  "A deploy goes smoothly for once.",
];

export const processes = {
  fortune: {
    id: "fortune",
    handler: async (ctx, msg) => {
      const index = (await kv.get("index")) ?? 0;
      const fortune = FORTUNES[index % FORTUNES.length];
      await kv.set("index", index + 1);
      return fortune;
    },
  },
};
