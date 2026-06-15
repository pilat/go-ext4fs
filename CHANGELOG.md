# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-06-14

### Added

- HTree (`dir_index`) directory support. Directories that outgrow a single block
  are now emitted as valid depth-1 hash-tree indexes, so files in large
  directories open by name on a mounted kernel instead of falling back to a
  linear scan. The directory hash is a byte-exact half_md4 (signed and unsigned),
  validated against `e2fsprogs`. The `dir_index` feature and hash-signedness flag
  are recorded only when a directory is actually indexed, so images without large
  directories are byte-for-byte unchanged.
- Maintenance of htree directories in foreign images opened with `Open`. Adding
  to an indexed directory now flattens and rebuilds its index with the image's
  own hash seed and signedness instead of overwriting it; deletes are left as the
  kernel does, untouched.
- `WithChecksum()` option to write `metadata_csum` images. When enabled, every
  superblock, group descriptor, block/inode bitmap, inode, and directory block
  carries a valid CRC32C checksum, so `e2fsck` and a real kernel accept the image
  with the `metadata_csum` feature. It is off by default and the default output is
  byte-for-byte unchanged; reopening a checksummed image and appending to it keeps
  the checksums valid. Directories under `metadata_csum` stay linear (htree's
  `dx_tail` index checksum is not yet written); mutating an existing htree
  directory, extended attributes, external extent trees and resize are refused
  rather than emit an image `e2fsck` would fault.
- `Open` now accepts foreign `metadata_csum` images of matching geometry, including
  stock `mke2fs` images that store the checksum seed explicitly
  (`metadata_csum_seed`) — even after a `tune2fs -U` has decoupled the seed from
  the UUID.
- Fuzz target (`FuzzChecksumOps`) driving a checksummed image through arbitrary
  operation sequences and a reopen cycle, asserting every metadata checksum stays
  self-consistent and the free-block counts match the bitmaps.

### Changed

- `Open` now decides reopen acceptance from a feature whitelist and refuses any
  image it cannot correctly rewrite: an unsupported RO_COMPAT feature (gdt_csum,
  bigalloc, quota) or reserved GDT blocks (the online-resize `resize_inode` layout).
  `metadata_csum` is the one formerly-refused feature now accepted, because its
  checksums are maintained on every write.

### Fixed

- Adding an entry to a hash-indexed directory previously overwrote its dx_root
  index on the first insert — a silent corruption reachable once `Open` accepted
  foreign images. Such inserts now flatten the directory and re-index it at save.
- The free-inode and free-block counts are now correct after reopening an image
  whose directory entries or files were deleted by a previous session or the
  kernel (inodes and blocks freed below the allocation high-water mark were
  previously miscounted as used, producing counts `e2fsck` rejects).

## [1.0.1] - 2026-06-10

### Added

- Fuzz target (`FuzzOps`) driving the public API with arbitrary operation
  sequences, plus a `make fuzz` target and a committed regression corpus.
- Kernel-verified extent-tree e2e test: a file spanning five block groups must
  produce a depth-1 extent tree, with the extent count pinned via `filefrag`.
- Byte-level unit tests for multi-leaf extent-tree splitting (340+ extents),
  unreachable through the public API in CI-sized images.
- Superblock geometry validation on `Open`: blocks-per-group and
  inodes-per-group must match the library's fixed layout.

### Fixed

- Operations against freed (stale) inode numbers are now rejected instead of
  corrupting the filesystem. Reallocating a freed directory inode inside its
  own former subtree could create a self-referencing directory and crash
  `DeleteDirectory` with a stack overflow (found by fuzzing).
- `CreateDirectory` and `CreateSymlink` now reject names that already exist in
  the parent directory; previously they appended a duplicate directory entry.
- `CreateFile` now refuses to overwrite an existing directory or symlink;
  previously it silently morphed the inode into a regular file, orphaning any
  subtree and leaving a stale file type in the parent directory entry.
- Unit tests, the fuzz corpus and benchmarks now run without Docker; only the
  kernel-backed e2e tests are skipped (previously the whole test binary
  silently exited green).

### Changed

- Renamed three e2e tests whose names promised extent-tree coverage they no
  longer delivered after the v1.0.0 allocator rework: `ExtentTreeConversion` →
  `ManySmallFiles`, `ExtentTreeLeafAllocation` → `FreedBlockRunReuse`,
  `ExtentTreeManyExtents` → `LargeFileAfterFragmentation`.

## [1.0.0] - 2026-06-08

### Added

- Initial stable release: pure Go ext4 image writer with extent-based mapping,
  extended attributes (SELinux, POSIX ACLs, capabilities), fast and slow
  symlinks, hardlinks, custom volume labels, two-way resize within a 16 GiB
  bracket, reproducible images via `WithCreatedAt`, and `Open` support for
  images created by this library.

[1.1.0]: https://github.com/pilat/go-ext4fs/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/pilat/go-ext4fs/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/pilat/go-ext4fs/releases/tag/v1.0.0
