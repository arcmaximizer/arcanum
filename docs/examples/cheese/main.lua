-- Cheeseboard is a game where you try to find your opponent's cheese.
-- First to find the cheese wins!

local handlers = {
  board = {
    -- Don't change the template ID
    id = "board",
    handler = "./cheeseboard.lua",
  },
  entrypoint = {
    id = "entrypoint",
    handler = function(ctx, msg)
      -- Matchmaker
      if msg.type = "enter" then
        return enterMatch(ctx, msg.id)
      end
    end
  }
}

local function enterMatch(ctx, id)
  -- Throws an error if the id is used by a different process
  local board = spawn("board", id)
  board.call("register", ctx.from)
end

return handlers
