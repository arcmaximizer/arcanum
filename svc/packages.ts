// Handles tarballs
import { Hash } from "../lib/types.ts";
import { Result } from "neverthrow";

export class BundleCorruptError extends Error {}
export class BundleNotExistsError extends Error {}
export class BundleNoEntrypointError extends Error {}
export class AssetNotExistsError extends Error {}

export interface PackageStore {
  getEntrypoint(packageHash: Hash): Promise<Result<Blob, AssetNotExistsError>>;
  getBundle(packageHash: Hash): Promise<Result<Blob, BundleNotExistsError>>;
  getBundleHash(tarball: Blob): Promise<Hash>;
  getAsset(
    packageHash: Hash,
    assetPath: string,
  ): Promise<Result<Blob, AssetNotExistsError>>;

  // Saves the tarball to the filesystem and returns hash
  addBundle(
    tarball: Blob,
  ): Promise<Result<Hash, BundleNoEntrypointError | BundleCorruptError>>;
  deleteBundle(packageHash: Hash): Promise<Result<void, BundleNotExistsError>>;
}
