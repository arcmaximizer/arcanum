import { Hash } from "../lib/types.ts";
import { err, ok, Result } from "neverthrow";
import path from "path";
import { typeByExtension } from "@std/media-types/type-by-extension";

const OVERRIDES: Record<string, string> = {
  ".ts": "text/typescript",
};

export class BundleCorruptError extends Error {}
export class BundleNotFoundError extends Error {}
export class BundleNoEntrypointError extends Error {}
export class AssetNotFoundError extends Error {}
export class FilesystemError extends Error {}

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
  ): Promise<Result<Blob, AssetNotFoundError>>;

  // Saves the tarball to the filesystem and returns hash
  addBundle(
    tarball: Blob,
  ): Promise<Result<Hash, BundleNoEntrypointError | BundleCorruptError>>;
  deleteBundle(packageHash: Hash): Promise<Result<void, BundleNotFoundError>>;
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
      // Better fail early when something like that is messed up
      if (
        err instanceof Deno.errors.NotFound ||
        err instanceof Deno.errors.IsADirectory
      ) {
        continue;
      } else {
        return err(new FilesystemError());
      }
    }
  }

  return err(new AssetNotFoundError));
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

    const packageExists = await Deno.stat(packageDir);

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
    }, (err) => {
      return err;
    })
    
    if (m instanceof File) return ok(m);
    if (m instanceof FilesystemError) return err(m)
    if (m instanceof AssetNotFoundError) return err(new BundleNoEntrypointError())
    
    return err(new FilesystemError())
  }

  async getBundle(packageHash: Hash) {
    const tarball = path.join(this.root, "tarballs", packageHash + ".tar.gz")
    try {
      const data = await Deno.readFile(tarball)
      return new File([data], packageHash + ".tar.gz")
    } catch (e) {
      if (e instanceof Deno.errors.NotFound) return err(new BundleNotFoundError());
      return err(new FilesystemError())
    }
  }
}
