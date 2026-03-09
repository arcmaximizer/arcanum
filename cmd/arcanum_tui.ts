#!/usr/bin/env -S deno run --allow-all

import { Database } from "@db/sqlite";
import { Kysely } from "kysely";
import { keypress } from "jsr:@cliffy/keypress";
import { DenoSqliteDialect } from "../lib/db_adapter.ts";
import {
  addNode,
  createCheckpoint,
  getHead,
  getHeads,
  initTreeTables,
  nodes,
  type TreeDatabase,
} from "../svc/store.ts";

interface TreeNode {
  id: string;
  parent: string | null;
  base: string | null;
  checkpointId: string | null;
}

interface KvWrite {
  key: string;
  value: string | null;
}

interface TreeData {
  rootNodes: string[];
  nodeMap: Map<string, TreeNode>;
  childrenMap: Map<string, string[]>;
}

class ArcanumTui {
  #db: Kysely<TreeDatabase>;
  #currentTreeId: string = "main";
  #treeIds: string[] = [];
  #treeData: Map<string, TreeData> = new Map();
  #selectedIndex: number = 0;
  #visibleNodes: string[] = [];
  #mode: "browse" | "delete" | "help" = "browse";

  constructor(dbPath: string) {
    const db = new Database(dbPath);
    this.#db = new Kysely<TreeDatabase>({
      dialect: new DenoSqliteDialect({
        database: db,
        onCreateConnection: async (conn) => {
          await conn.executeQuery({
            sql: "PRAGMA foreign_keys = ON",
            parameters: [],
            queryId: { queryId: "" },
            query: { kind: "RawNode" } as any,
          });
        },
      }),
    });
  }

  async init() {
    await initTreeTables(this.#db);
    await this.#loadData();
  }

  async #loadData() {
    const heads = await getHeads(this.#db);
    this.#treeIds = Array.from(heads.keys());
    if (this.#treeIds.length === 0) {
      this.#treeIds = ["main"];
    }
    if (!this.#treeIds.includes(this.#currentTreeId)) {
      this.#currentTreeId = this.#treeIds[0]!;
    }

    for (const treeId of this.#treeIds) {
      const allNodes = await nodes(this.#db);
      const nodeMap = new Map<string, TreeNode>();
      const childrenMap = new Map<string, string[]>();

      for (const nodeId of allNodes) {
        const nodeData = await this.#db
          .selectFrom("nodes")
          .selectAll()
          .where("id", "=", nodeId)
          .executeTakeFirst();

        if (nodeData) {
          nodeMap.set(nodeId, {
            id: nodeData.id,
            parent: nodeData.parent ?? null,
            base: nodeData.base ?? null,
            checkpointId: nodeData.checkpoint_id ?? null,
          });

          const parent = nodeData.parent;
          if (parent) {
            const children = childrenMap.get(parent) ?? [];
            children.push(nodeId);
            childrenMap.set(parent, children);
          }
        }
      }

      const rootNodes = allNodes
        .filter((id) => !nodeMap.get(id)?.parent)
        .sort();

      this.#treeData.set(treeId, { rootNodes, nodeMap, childrenMap });
    }

    this.#updateVisibleNodes();
  }

  #updateVisibleNodes() {
    const treeData = this.#treeData.get(this.#currentTreeId);
    if (!treeData) {
      this.#visibleNodes = [];
      return;
    }

    const nodes: string[] = [];
    const { rootNodes, childrenMap } = treeData;

    const traverse = (nodeId: string) => {
      nodes.push(nodeId);
      const children = childrenMap.get(nodeId) ?? [];
      for (const child of children) {
        traverse(child);
      }
    };

    for (const root of rootNodes) {
      traverse(root);
    }

    this.#visibleNodes = nodes;
    if (this.#selectedIndex >= this.#visibleNodes.length) {
      this.#selectedIndex = Math.max(0, this.#visibleNodes.length - 1);
    }
  }

  async #render() {
    const lines: string[] = [];

    lines.push(
      "\x1b[1;34m╔══════════════════════════════════════════════════════════════╗\x1b[0m",
    );
    lines.push(
      "\x1b[1;34m║\x1b[0m \x1b[1;36mArcanum Event Tree TUI\x1b[0m                                       \x1b[1;34m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;34m╚══════════════════════════════════════════════════════════════╝\x1b[0m",
    );

    lines.push(
      `\x1b[33m[Trees: ${
        this.#treeIds.join(" | ")
      }]\x1b[0m \x1b[90m(Tab to switch)\x1b[0m`,
    );
    lines.push(`\x1b[90mCurrent: ${this.#currentTreeId}\x1b[0m`);
    lines.push("");

    if (this.#mode === "help") {
      this.#renderHelp(lines);
    } else if (this.#mode === "delete") {
      this.#renderDelete(lines);
    } else {
      this.#renderBrowse(lines);
    }

    console.clear();
    console.log(lines.join("\n"));
  }

  #renderHelp(lines: string[]) {
    lines.push("\x1b[1;32m╔════════════════════════════════════════╗\x1b[0m");
    lines.push(
      "\x1b[1;32m║\x1b[0m          \x1b[36mKeyboard Shortcuts\x1b[0m            \x1b[1;32m║\x1b[0m",
    );
    lines.push("\x1b[1;32m╠════════════════════════════════════════╣\x1b[0m");
    lines.push(
      "\x1b[1;32m║\x1b[0m  ↑/↓     Navigate events               \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  Tab     Switch trees                  \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  a       Add new event (prompts)       \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  e       Add child event (prompts)     \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  d       Delete event                  \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  r       Refresh data                  \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  h       Toggle help                   \x1b[1;32m║\x1b[0m",
    );
    lines.push(
      "\x1b[1;32m║\x1b[0m  q       Quit                          \x1b[1;32m║\x1b[0m",
    );
    lines.push("\x1b[1;32m╚════════════════════════════════════════╝\x1b[0m");
    lines.push("");
    lines.push("\x1b[90mPress any key to return...\x1b[0m");
  }

  #renderDelete(lines: string[]) {
    lines.push("\x1b[1;31m🗑️  Delete Event\x1b[0m");
    lines.push("\x1b[90m────────────────────────────────────────\x1b[0m");
    lines.push("");

    const selectedNode = this.#visibleNodes[this.#selectedIndex];
    if (selectedNode) {
      const treeData = this.#treeData.get(this.#currentTreeId);
      const node = treeData?.nodeMap.get(selectedNode);

      lines.push(`\x1b[33mEvent ID:\x1b[0m ${selectedNode}`);
      lines.push(`\x1b[33mParent:\x1b[0m ${node?.parent ?? "(none)"}`);
      lines.push(`\x1b[33mBase:\x1b[0m ${node?.base ?? "(none)"}`);
      lines.push(
        `\x1b[33mCheckpoint:\x1b[0m ${node?.checkpointId ?? "(none)"}`,
      );
      lines.push("");
      lines.push(
        "\x1b[1;31mPress 'y' to confirm, any other key to cancel\x1b[0m",
      );
    }
  }

  async #renderBrowse(lines: string[]) {
    if (this.#visibleNodes.length === 0) {
      lines.push("\x1b[90mNo events in tree. Press 'a' to add one.\x1b[0m");
      return;
    }

    const treeData = this.#treeData.get(this.#currentTreeId);
    if (!treeData) return;

    const { nodeMap, childrenMap } = treeData;

    const traverseRec = (
      nodeId: string,
      depth: number,
      isLast: boolean,
      ancestorLine: boolean[],
    ) => {
      const children = childrenMap.get(nodeId) ?? [];

      let line = "";
      for (let i = 0; i < depth; i++) {
        if (ancestorLine[i]) {
          line += "│   ";
        } else {
          line += "    ";
        }
      }
      line += depth === 0 ? "" : (isLast ? "└── " : "├── ");

      const isSelected = this.#visibleNodes[this.#selectedIndex] === nodeId;

      if (isSelected) {
        line = "\x1b[1;32m▶\x1b[0m " + line + nodeId;
      } else {
        line = "  " + line + nodeId;
      }
      lines.push(line);

      const newAncestorLine = [...ancestorLine, !isLast];
      for (let i = 0; i < children.length; i++) {
        const child = children[i];
        if (child) {
          traverseRec(
            child,
            depth + 1,
            i === children.length - 1,
            newAncestorLine,
          );
        }
      }
    };

    const rootNodes = treeData.rootNodes;
    for (let i = 0; i < rootNodes.length; i++) {
      const root = rootNodes[i];
      if (root) traverseRec(root, 0, i === rootNodes.length - 1, []);
    }

    const selectedNode = this.#visibleNodes[this.#selectedIndex];
    if (selectedNode) {
      const node = nodeMap.get(selectedNode);
      lines.push("");
      lines.push(
        "\x1b[1;36m═══════════════ Event Details ═══════════════\x1b[0m",
      );
      lines.push(`\x1b[33mID:\x1b[0m ${selectedNode}`);
      lines.push(`\x1b[33mParent:\x1b[0m ${node?.parent ?? "(none)"}`);
      lines.push(`\x1b[33mBase:\x1b[0m ${node?.base ?? "(none)"}`);
      lines.push(
        `\x1b[33mCheckpoint:\x1b[0m ${node?.checkpointId ?? "(none)"}`,
      );

      const kvWrites = await this.#getKvWrites(selectedNode);
      if (kvWrites.length > 0) {
        lines.push("");
        lines.push("\x1b[33mKV Writes:\x1b[0m");
        for (const kv of kvWrites) {
          lines.push(`  ${kv.key} = ${kv.value ?? "(deleted)"}`);
        }
      }
    }
  }

  async #getKvWrites(eventId: string): Promise<KvWrite[]> {
    const writes = await this.#db
      .selectFrom("kv_writes")
      .selectAll()
      .where("event_id", "=", eventId)
      .execute();
    return writes.map((w) => ({ key: w.key, value: w.value }));
  }

  #findDeepestHead(treeData: TreeData): string {
    const { rootNodes, childrenMap } = treeData;
    if (rootNodes.length === 0) return "";

    let current: string = rootNodes[0]!;
    let hasChildren = true;

    while (hasChildren) {
      const children = childrenMap.get(current);
      if (children && children.length > 0) {
        current = children[children.length - 1]!;
      } else {
        hasChildren = false;
      }
    }

    return current;
  }

  async #handleKeyPress(event: { key?: string; sequence?: string }) {
    const key = event.key || event.sequence || "";

    if (this.#mode === "help") {
      this.#mode = "browse";
      return;
    }

    if (this.#mode === "delete") {
      if (key === "y" || key === "Y") {
        const selectedNode = this.#visibleNodes[this.#selectedIndex];
        if (selectedNode) {
          try {
            await this.#db.deleteFrom("nodes").where("id", "=", selectedNode)
              .execute();
            await this.#loadData();
          } catch (e) {
            console.log(`\x1b[31mError: ${e}\x1b[0m`);
          }
        }
      }
      this.#mode = "browse";
      return;
    }

    switch (key) {
      case "ArrowUp":
      case "k":
        this.#selectedIndex = Math.max(0, this.#selectedIndex - 1);
        break;
      case "ArrowDown":
      case "j":
        this.#selectedIndex = Math.min(
          this.#visibleNodes.length - 1,
          this.#selectedIndex + 1,
        );
        break;
      case "?":
        this.#mode = "help";
        break;
      case "Tab": {
        const currentIdx = this.#treeIds.indexOf(this.#currentTreeId);
        this.#currentTreeId = this
          .#treeIds[(currentIdx + 1) % this.#treeIds.length]!;
        this.#selectedIndex = 0;
        this.#updateVisibleNodes();
        break;
      }
      case "a":
        await this.#handleAddEvent();
        break;
      case "e":
        await this.#handleAddChild();
        break;
      case "d":
        if (this.#visibleNodes.length > 0) {
          this.#mode = "delete";
        }
        break;
      case "r":
        await this.#loadData();
        break;
      case "h":
        this.#mode = "help";
        break;
      case "q":
        console.log("\x1b[0m");
        await this.#db.destroy();
        Deno.exit(0);
    }
  }

  async #handleAddEvent() {
    const eventId = prompt("Event ID:");
    if (!eventId) return;

    const parentId = prompt("Parent ID (leave empty for root):") || undefined;
    const kvInput = prompt(
      "KV diffs (key=value, one per line, empty to finish):",
    );

    const kvDiffs = new Map<string, string | null>();
    if (kvInput) {
      for (const line of kvInput.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const eqIdx = trimmed.indexOf("=");
        if (eqIdx > 0) {
          const k = trimmed.substring(0, eqIdx).trim();
          const v = trimmed.substring(eqIdx + 1).trim();
          kvDiffs.set(k, v || null);
        }
      }
    }

    try {
      await addNode(this.#db, eventId, parentId, kvDiffs);
      await this.#loadData();
      const newIndex = this.#visibleNodes.indexOf(eventId);
      if (newIndex >= 0) {
        this.#selectedIndex = newIndex;
      }
    } catch (e) {
      console.log(`\x1b[31mError: ${e}\x1b[0m`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  async #handleAddChild() {
    const parentNode = this.#visibleNodes[this.#selectedIndex];
    if (!parentNode) return;

    const eventId = prompt("Event ID:");
    if (!eventId) return;

    const kvInput = prompt(
      "KV diffs (key=value, one per line, empty to finish):",
    );

    const kvDiffs = new Map<string, string | null>();
    if (kvInput) {
      for (const line of kvInput.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const eqIdx = trimmed.indexOf("=");
        if (eqIdx > 0) {
          const k = trimmed.substring(0, eqIdx).trim();
          const v = trimmed.substring(eqIdx + 1).trim();
          kvDiffs.set(k, v || null);
        }
      }
    }

    try {
      await addNode(this.#db, eventId, parentNode, kvDiffs);
      await this.#loadData();
      const newIndex = this.#visibleNodes.indexOf(eventId);
      if (newIndex >= 0) {
        this.#selectedIndex = newIndex;
      }
    } catch (e) {
      console.log(`\x1b[31mError: ${e}\x1b[0m`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  async run() {
    await this.init();

    console.log("\x1b[?1049h");
    await this.#render();

    for await (const event of keypress()) {
      await this.#handleKeyPress(event);
      await this.#render();
    }
  }
}

const dbPath = Deno.args[0] || "./arcanum.db";
const tui = new ArcanumTui(dbPath);
await tui.run();
