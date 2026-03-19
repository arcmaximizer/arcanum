import { Hash } from "../lib/types.ts";
import { err, ok, Result } from "neverthrow";
import path, { dirname } from "node:path";
import { typeByExtension } from "@std/media-types/type-by-extension";
import { UntarStream } from "@std/tar/untar-stream";
const OVERRIDES: Record<string, string> = {
  ".ts": "text/typescript",
};

export class BundleCorruptError extends Error {}
export class BundleNotFoundError extends Error {}
export class BundleNoEntrypointError extends Error {}
export class AssetNotFoundError extends Error {}
export class FilesystemError extends Error {}
export class InvalidInputError extends Error {}

export interface PackageStore {
  getEntrypoint(
    packageHash: Hash,
  ): Promise<Result<Blob, BundleNoEntrypointError>>;
  getBundle(
    packageHash: Hash,
  ): Promise<Result<Blob, BundleNotFoundError | FilesystemError>>;
  getBundleHash(tarball: Blob): Promise<Hash>;
  getAsset(
    packageHash: Hash,
    assetPath: string,
  ): Promise<
    Result<Blob, AssetNotFoundError | BundleNotFoundError | FilesystemError>
  >;

  bundleExists(packageHash: Hash): Promise<boolean>;
  assetExists(packageHash: Hash, assetPath: string): Promise<boolean>;

  // Saves the tarball to the filesystem and returns hash
  addBundle(
    tarball: Blob,
  ): Promise<
    Result<Hash, BundleNoEntrypointError | BundleCorruptError | FilesystemError>
  >;
  deleteBundle(
    packageHash: Hash,
  ): Promise<Result<void, BundleNotFoundError | FilesystemError>>;
}

async function readOf(
  dir: string,
  candidates: string[],
): Promise<Result<File, AssetNotFoundError | FilesystemError>> {
  for (const name of candidates) {
    try {
      const file = await Deno.readFile(`${dir}/${name}`);
      const ext = path.extname(name);
      const type = OVERRIDES[ext] ?? typeByExtension(ext) ??
        "application/octet-stream";
      return ok(new File([file], name, { type }));
    } catch (e) {
      // We want to break if any has an error other than "file not found"
      if (
        e instanceof Deno.errors.NotFound ||
        e instanceof Deno.errors.IsADirectory
      ) {
        continue;
      } else {
        return err(new FilesystemError());
      }
    }
  }

  return err(new AssetNotFoundError());
}

export async function isGzip(blob: Blob): Promise<boolean> {
  const slice = blob.slice(0, 2);
  const header = new Uint8Array(await slice.arrayBuffer());
  return header[0] === 0x1f && header[1] === 0x8b;
}

function sanitizePath(entryPath: string): string | null {
  const normalized = path.normalize(entryPath);
  // Reject absolute paths, paths starting with "..", or containing drive letters
  if (
    path.isAbsolute(normalized) || normalized.startsWith("..") ||
    /^[A-Za-z]:/.test(normalized)
  ) {
    return null;
  }
  return normalized;
}

/*
  basic filesystem design, where @ is root:
  @/tarballs - store all tarballs in tar.xz files
  @/bundles - store all bundles in folders
*/
export class FilesystemPackageStore implements PackageStore {
  constructor(readonly root: string) {}

  async getEntrypoint(packageHash: Hash) {
    const packageDir = path.join(this.root, "bundles", packageHash);

    const file = await readOf(packageDir, [
      "entrypoint.ts",
      "entrypoint.js",
      "main.ts",
      "main.js",
      "index.ts",
      "index.js",
    ]);

    const m = file.match((res) => {
      return res;
    }, (err) => {
      return err;
    });

    if (m instanceof File) return ok(m);
    if (m instanceof FilesystemError) return err(m);
    if (m instanceof AssetNotFoundError) {
      return err(new BundleNoEntrypointError());
    }

    return err(new FilesystemError());
  }

  async getBundle(packageHash: Hash) {
    const tarball = path.join(this.root, "tarballs", packageHash + ".tar.gz");
    try {
      const data = await Deno.readFile(tarball);
      return ok(new File([data], packageHash + ".tar.gz"));
    } catch (e) {
      if (e instanceof Deno.errors.NotFound) {
        return err(new BundleNotFoundError());
      }
      return err(new FilesystemError());
    }
  }

  async getBundleHash(blob: Blob): Promise<Hash> {
    const buffer = await blob.arrayBuffer();
    const hashBuffer = await crypto.subtle.digest("SHA-256", buffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    const hashHex = hashArray
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");

    return hashHex as Hash;
  }

  async getAsset(
    packageHash: Hash,
    assetPath: string,
  ) {
    if (!this.bundleExists(packageHash)) return err(new BundleNotFoundError());

    const px = path.normalize(
      path.join(this.root, "bundles", packageHash, assetPath),
    );

    if (!px.startsWith(path.join(this.root, "bundles", packageHash))) {
      return err(new InvalidInputError());
    }
    try {
      const data = await Deno.readFile(px);
      return ok(new File([data], assetPath));
    } catch (e) {
      if (e instanceof Deno.errors.NotFound) {
        return err(new AssetNotFoundError());
      }
      return err(new FilesystemError());
    }
  }

  async bundleExists(packageHash: Hash) {
    const px = path.normalize(
      path.join(this.root, "bundles", packageHash),
    );

    try {
      await Deno.lstat(px);
      return true;
    } catch (e) {
      return false;
    }
  }
  async assetExists(packageHash: Hash, assetPath: string) {
    const px = path.normalize(
      path.join(this.root, "bundles", packageHash, assetPath),
    );

    if (!px.startsWith(path.join(this.root, "bundles", packageHash))) {
      return false;
    }

    try {
      await Deno.lstat(px);
      return true;
    } catch (e) {
      return false;
    }
  }

  async addBundle(tarball: Blob) {
    const hash = await this.getBundleHash(tarball);
    const tarballPath = path.normalize(
      path.join(this.root, "tarballs", hash),
    );

    const outDir = path.join(this.root, "bundles", hash);
    const arrayBuffer = await tarball.arrayBuffer();
    const bytes = new Uint8Array(arrayBuffer);
    try {
      await Deno.writeFile(tarballPath, bytes);

      // Unpack the tarball, decompressing if needed
      const gzip = await isGzip(tarball);
      let stream = tarball.stream();

      if (gzip) stream = stream.pipeThrough(new DecompressionStream("gzip"));

      for await (const entry of stream.pipeThrough(new UntarStream())) {
        const normalized = sanitizePath(entry.path);
        if (normalized === null) {
          return err(new BundleCorruptError());
        }

        const dest = path.join(outDir, normalized);
        await Deno.mkdir(dirname(dest), { recursive: true });

        if (entry.readable) {
          await entry.readable.pipeTo((await Deno.create(dest)).writable);
        }
      }

      // Check for entrypoint
      const entrypointResult = await this.getEntrypoint(hash);
      if (entrypointResult.isErr()) {
        return err(new BundleNoEntrypointError());
      }

      return ok(hash);
    } catch (e) {
      return err(new FilesystemError());
    }
  }

  async deleteBundle(packageHash: Hash) {
    const bundleDir = path.join(this.root, "bundles", packageHash);
    const tarballPath = path.join(this.root, "tarballs", packageHash);

    try {
      await Deno.remove(bundleDir, { recursive: true });
      await Deno.remove(tarballPath);
      return ok(undefined);
    } catch (e) {
      if (e instanceof Deno.errors.NotFound) {
        return err(new BundleNotFoundError());
      }
      return err(new FilesystemError());
    }
  }
}
