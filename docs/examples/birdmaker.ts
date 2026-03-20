// Birdmaker is an app that allows you to create your own birds.

const prefixes = [
  "Blue-eyed",
  "Brown-footed",
  "Sharp-eared",
  "Atlantic",
  "Arctic",
  "Eurasian",
  "Red-nosed",
];
const suffixes = [
  "skylark",
  "chicken",
  "woodpecker",
  "booby",
  "reindeer",
  "eagle",
  "duck",
];

interface BirdData {
  name: string;
  grade: 1 | 2 | 3 | 4 | 5;
  createdAt: number;
}

function makeName() {
  const prefix = prefixes[Math.floor(Math.random() * prefixes.length)];
  const suffix = suffixes[Math.floor(Math.random() * suffixes.length)];

  return `${prefix} ${suffix}`;
}

export default async function (from, req, ctx) {
  switch (req.command) {
    case "show": {
      if (!req.birdName) throw { type: "NO_BIRD_NAME" };

      const birdData: BirdData | undefined = await ctx.get(req.birdName);
      if (!birdData) throw { type: "NO_BIRD" };
      return birdData;
    }
    case "make": {
      // Make a bird
      const name = makeName();
      const grade = Math.ceil(Math.random() * 5);

      // Check if the bird exists
      const birdData: BirdData | undefined = await ctx.get(name);
      if (birdData) return birdData;

      const newData = {
        name,
        grade,
        createdAt: Date.now(),
      };
      await ctx.set(name, newData);

      return newData;
    }
    case "squawk": {
      if (!req.birdName) throw { type: "NO_BIRD_NAME" };

      const birdExists: boolean = await ctx.exists(name);
      if (!birdExists) throw { type: "NO_BIRD" };

      if (req.birdName.endsWith("duck")) return "quack";
      if (req.birdName.endsWith("eagle")) return "squawk";
      if (req.birdName.endsWith("reindeer")) return "neigh";
      if (req.birdName.endsWith("chicken")) return "cluck";
      return "chirp";
    }
    default: {
      throw {
        type: "NO_COMMAND",
      };
    }
  }
}
