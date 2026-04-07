import { Hash, hashBlob } from "../types.ts";
import { err, ok, Result } from "neverthrow";
import { UntarStream } from "@std/tar/untar-stream";

export class FilesystemError extends Error {}
export class PackageNotFoundError extends Error {}
export class PackageExistsError extends Error {}
export class AssetNotFoundError extends Error {}

export interface PackageStore {
  addTarball(
    tarball: Blob,
  ): Promise<Result<Hash, PackageExistsError | FilesystemError>>;
  pruneTarball(
    hash: Hash,
  ): Promise<Result<void, PackageNotFoundError | FilesystemError>>;
  getTarball(
    hash: Hash,
  ): Promise<Result<Blob, PackageNotFoundError | FilesystemError>>;
  getEntrypoint(
    hash: Hash,
  ): Promise<Result<
    Blob,
    PackageNotFoundError | AssetNotFoundError | FilesystemError
  >>;
  getAsset(
    bundle: Hash,
    asset: string,
  ): Promise<Result<
    Blob,
    FilesystemError | PackageNotFoundError | AssetNotFoundError
  >>;
}

export class MemoryPackageStore implements PackageStore {
  private tarballs: Map<Hash, Blob> = new Map();

  async addTarball(tarball: Blob) {
    const hash = await hashBlob(tarball);

    if (this.tarballs.has(hash)) return err(new PackageExistsError());
    this.tarballs.set(hash, tarball);

    return ok(hash);
  }

  async pruneTarball(hash: Hash) {
    if (!this.tarballs.has(hash)) return err(new PackageNotFoundError());
    this.tarballs.delete(hash);
    return ok();
  }

  async getTarball(hash: Hash) {
    const tarball = this.tarballs.get(hash);
    if (!tarball) return err(new PackageNotFoundError());

    return ok(tarball);
  }

  async getAssets(hash: Hash, assets: string[]) {
    const tarballResult = await this.getTarball(hash);
    if (tarballResult.isErr()) return tarballResult as never;
    const blob = tarballResult.value;

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
  }

  async getAsset(hash: Hash, asset: string) {
    const assetsResult = await this.getAssets(hash, [asset]);
    if (assetsResult.isErr()) return assetsResult as never;
    const v = assetsResult.value;
    const data = Object.values(v)[0];
    return data ? ok(data) : err(new AssetNotFoundError());
  }

  async getEntrypoint(hash: Hash) {
    const assetsResult = await this.getAssets(hash, ["entrypoint.ts", "entrypoint.js"]);
    if (assetsResult.isErr()) return assetsResult as never;
    const v = assetsResult.value;
    const data = v["entrypoint.ts"] ?? v["entrypoint.js"];
    return data ? ok(data) : err(new AssetNotFoundError());
  }
}
