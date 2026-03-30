// Naive package store - stores everything in memory
import { Hash, hashBlob, wrap } from "../types.ts";
import { err, ok, ResultAsync } from "neverthrow";
import { UntarStream } from "@std/tar/untar-stream";

export class FilesystemError extends Error {}
export class PackageNotFoundError extends Error {}
export class PackageExistsError extends Error {}
export class AssetNotFoundError extends Error {}

export interface PackageStore {
  addTarball(
    tarball: Blob,
  ): ResultAsync<Hash, PackageExistsError | FilesystemError>;
  pruneTarball(
    hash: Hash,
  ): ResultAsync<void, PackageNotFoundError | FilesystemError>;
  getTarball(
    hash: Hash,
  ): ResultAsync<Blob, PackageNotFoundError | FilesystemError>;
  getEntrypoint(
    hash: Hash,
  ): ResultAsync<
    Blob,
    PackageNotFoundError | AssetNotFoundError | FilesystemError
  >;
  getAsset(
    bundle: Hash,
    asset: string,
  ): ResultAsync<
    Blob,
    FilesystemError | PackageNotFoundError | AssetNotFoundError
  >;
}

export class MemoryPackageStore implements PackageStore {
  private tarballs: Map<Hash, Blob> = new Map();

  addTarball(tarball: Blob) {
    return wrap(async () => {
      const hash = await hashBlob(tarball);

      if (this.tarballs.has(hash)) return err(new PackageExistsError());
      this.tarballs.set(hash, tarball);

      return ok(hash);
    });
  }

  pruneTarball(hash: Hash) {
    return wrap(async () => {
      if (!this.tarballs.has(hash)) return err(new PackageNotFoundError());
      this.tarballs.delete(hash);
      return ok();
    });
  }

  getTarball(hash: Hash) {
    return wrap(async () => {
      const tarball = this.tarballs.get(hash);
      if (!tarball) return err(new PackageNotFoundError());

      return ok(tarball);
    });
  }

  getAssets(hash: Hash, assets: string[]) {
    return this.getTarball(hash).andThen((blob) =>
      wrap(async () => {
        const stream = blob.stream().pipeThrough(
          new DecompressionStream("gzip"),
        ).pipeThrough(new UntarStream());

        const blobs: { [k: string]: Blob } = {};

        for await (const entry of stream) {
          if (assets.includes(entry.path)) {
            if (entry.readable) {
              const res = new Response(entry.readable);
              blobs[entry.path] = await res.blob();
            }
          }
        }
        return ok(blobs);
      })
    );
  }

  getAsset(hash: Hash, asset: string) {
    // It's a little terse, but it could be worse. -Arc
    return this.getAssets(hash, [asset]).andThen((v) => {
      const data = Object.values(v)[0];
      return data ? ok(data) : err(new AssetNotFoundError());
    });
  }

  getEntrypoint(hash: Hash) {
    return this.getAssets(hash, ["entrypoint.ts", "entrypoint.js"]).andThen(
      (v) => {
        const data = v["entrypoint.ts"] ?? v["entrypoint.js"];
        return data ? ok(data) : err(new AssetNotFoundError());
      },
    );
  }
}
