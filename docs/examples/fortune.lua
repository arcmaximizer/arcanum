local FORTUNES = {
  "You will write clean code today.",
  "A refactor is in your future.",
  "Someone will appreciate your commit messages.",
  "Your tests will pass on the first run.",
  "Today is a good day for a code review.",
  "Beware of hidden dependencies.",
  "The bug you're looking for is one line away.",
  "A deploy goes smoothly for once."
}

local processes = {
  fortune = {
    id = "fortune",
    handler = function(ctx, msg)
      local index = ctx.kv.get("index") or 0
      local fortune = FORTUNES[(index % #FORTUNES) + 1]
      ctx.kv.set("index", index + 1)
      return fortune
    end,
  },
}

return processes
