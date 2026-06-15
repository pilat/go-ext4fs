package ext4fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// Hash-tree (htree) directory rebuild. We do NOT reproduce the kernel's
// incremental dx_probe/do_split machinery; instead emitHtree deterministically
// rebuilds a whole directory as a valid depth-1 htree from its current entry set
// (Strategy A). The same engine serves both directions: our own large directories
// (own hash params) and foreign htree directories that were flattened before a
// mutation (foreign hash params). Layout reference: a depth-1 dx_root in logical
// block 0, K leaf blocks in logical blocks 1..K.

// dxRootLimit is the number of dx_root slots in a 4 KiB block with metadata_csum
// OFF: (blockSize - 12("." ) - 12("..") - 8(dx_root_info)) / 8. e2fsck checks this
// limit for exact equality (PR_2_HTREE_BAD_LIMIT), so emit must write it verbatim.
const dxRootLimit = (blockSize - 12 - 12 - 8) / 8 // 508

// dx_root field offsets within logical block 0.
const (
	dxRootInfoOffset   = 0x18 // dx_root_info (8 bytes)
	dxCountLimitOffset = 0x20 // {limit u16, count u16, block u32} — entry slot 0
	dxEntryArrayOffset = 0x28 // dx_entry[1..] {hash u32, block u32}
)

// errHtreeNotIndexable is the typed sentinel signalling that a directory cannot be
// represented as a depth-1 htree and must be LEFT LINEAR (which, after flatten, it
// already is). It covers every "cannot index, not an error" reason: an empty set,
// more than dxRootLimit leaves, a single hash-collision group too large for one
// leaf, an unsupported hash version, or not enough free space to grow the index.
// emitHtree guarantees NO side effects in all of these cases (each is detected
// before any block is freed or allocated), so emitHtreeDirs has one uniform
// "skip and stay linear" branch for own and foreign directories alike; only genuine
// I/O errors propagate and abort Save.
var errHtreeNotIndexable = errors.New("directory cannot be indexed as a depth-1 htree")

// hashedEntry pairs a directory entry with its computed (major, minor) hash.
type hashedEntry struct {
	entry        dirEntry
	major, minor uint32
}

// htreeLeafPlan is one planned leaf block: its entries (already sorted by
// (major, minor)) and the dx_entry boundary hash that routes to it (the minimum
// major hash among its entries; ignored for leaf 0, whose boundary is implicitly 0).
type htreeLeafPlan struct {
	entries      []dirEntry
	boundaryHash uint32
}

// dirRecLen returns the on-disk record length of a dirent with the given name
// length: the 8-byte header plus the name, rounded up to a 4-byte boundary.
func dirRecLen(nameLen int) int {
	rl := 8 + nameLen
	if rl%4 != 0 {
		rl += 4 - (rl % 4)
	}
	return rl
}

// packHtreeLeaves packs sorted hashed entries into leaf blocks. Entries sharing a
// major hash are never split across leaves (the canonical-layout rule that lets us
// clear every stored boundary's low bit and never need the htree continuation
// flag); leaf boundaries fall only between distinct major-hash values. It returns
// errHtreeNotIndexable if any single same-hash group exceeds one leaf.
// leafCapacity is the usable dirent bytes per leaf (blockSize with csum off).
func packHtreeLeaves(hes []hashedEntry, leafCapacity int) ([]htreeLeafPlan, error) {
	var (
		leaves   []htreeLeafPlan
		cur      []dirEntry
		curBytes int
		curMin   uint32
	)

	flush := func() {
		if len(cur) > 0 {
			leaves = append(leaves, htreeLeafPlan{entries: cur, boundaryHash: curMin})
			cur = nil
			curBytes = 0
		}
	}

	for i := 0; i < len(hes); {
		// Gather the whole group of entries sharing this major hash.
		j := i
		groupBytes := 0
		for j < len(hes) && hes[j].major == hes[i].major {
			groupBytes += dirRecLen(len(hes[j].entry.Name))
			j++
		}
		if groupBytes > leafCapacity {
			// A single hash value with more colliding entries than fit in one
			// leaf cannot be represented depth-1 with the clear-low-bit strategy.
			return nil, errHtreeNotIndexable
		}

		if curBytes+groupBytes > leafCapacity {
			flush()
		}
		if len(cur) == 0 {
			curMin = hes[i].major
		}
		for k := i; k < j; k++ {
			cur = append(cur, hes[k].entry)
		}
		curBytes += groupBytes
		i = j
	}
	flush()

	return leaves, nil
}

// liveEntryBytes is the leaf-block byte cost of a set of real directory entries
// (excluding "." and ".."). A directory whose entries exceed one leaf block
// (blockSize) is a candidate for htree indexing; one that fits stays linear.
func liveEntryBytes(entries []dirEntry) int {
	total := 0
	for _, e := range entries {
		total += dirRecLen(len(e.Name))
	}
	return total
}

// emitHtreeDirs is the single htree-write site, run by Save before
// finalizeMetadata (so index allocation precedes free-count finalization,
// decision 6). Indexing is a best-effort OPTIMIZATION: each registered directory
// whose live entries exceed one leaf block is rebuilt as a depth-1 htree when it
// can be, and any directory that cannot be — it fits one block, exceeds the
// depth-1 bound, has an oversized same-hash group, uses an unsupported hash
// version, or the image is out of free blocks — is LEFT LINEAR (which, after a
// mutation's flatten, it already is) and the loop continues. Own and foreign
// directories are treated identically; only own-origin indexing sets dirIndexUsed
// (which records the dir_index feature and signedness). Only genuine I/O errors
// propagate, so finalizeMetadata always runs and every directory in the final
// image is valid.
func (b *builder) emitHtreeDirs() error {
	// metadata_csum defers htree (the dx_tail index checksums are not implemented),
	// so under WithChecksum own directories stay linear multi-block — valid with a
	// det_checksum per leaf block. Foreign htree mutation is refused upstream in
	// addDirEntry/removeDirEntry, so reindexDirs holds only own dirs here.
	if b.csumEnabled {
		return nil
	}

	inodes := make([]uint32, 0, len(b.reindexDirs))
	for ino := range b.reindexDirs {
		inodes = append(inodes, ino)
	}
	// Emit in a deterministic order (map iteration is randomized) so block
	// allocation — and thus the resulting image bytes — are reproducible.
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	for _, dirInode := range inodes {
		// The directory may have been deleted (or its inode reused) during the
		// session; skip anything that is no longer a live directory. Loading the
		// inode here once (with its liveness) and passing it down avoids the
		// per-dir re-reads readAllEntries and commitHtreeLayout used to do.
		allocated, err := b.isInodeAllocated(dirInode)
		if err != nil {
			return err
		}
		if !allocated {
			continue
		}
		inode, err := b.readInode(dirInode)
		if err != nil {
			return err
		}
		if inode.Mode&0xF000 != s_IFDIR {
			continue
		}

		if err := b.indexDir(dirInode, inode, b.reindexDirs[dirInode]); err != nil {
			return err // best-effort skips return nil; only real I/O errors reach here
		}
	}

	return nil
}

// indexDir attempts to (re)index one live directory as a depth-1 htree, reusing the
// inode already loaded by emitHtreeDirs. Any reason the directory cannot be indexed
// leaves it linear and returns nil (indexing is an optimization); only genuine I/O
// errors are returned.
func (b *builder) indexDir(dirInode uint32, inode *inode, info reindexInfo) error {
	entries, parent, err := b.dirEntriesFromInode(inode, dirInode)
	if err != nil {
		return fmt.Errorf("failed to read directory %d for htree emit: %w", dirInode, err)
	}
	if liveEntryBytes(entries) <= blockSize {
		return nil // fits one leaf block — stay linear
	}

	version := b.defHashVersion
	if info.foreign {
		version = info.hashVersion
	}

	err = b.emitHtree(dirInode, inode, parent, entries, version)
	if errors.Is(err, errHtreeNotIndexable) {
		return nil // cannot index (own or foreign) — leave it linear
	}
	if err != nil {
		return fmt.Errorf("failed to emit htree for directory %d: %w", dirInode, err)
	}

	if !info.foreign {
		b.dirIndexUsed = true
	}
	return nil
}

// dirEntriesFromInode enumerates a directory's real entries (excluding "." and
// "..") and recovers its parent inode from the ".." record, working from an
// already-loaded inode so the emit loop does not re-read the inode and bitmap that
// readAllEntries would. It mirrors readAllEntries' block scan over the loaded inode.
func (b *builder) dirEntriesFromInode(inode *inode, dirInode uint32) ([]dirEntry, uint32, error) {
	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get directory blocks: %w", err)
	}

	var (
		entries     []dirEntry
		parentInode uint32
	)
	for _, blockNum := range dataBlocks {
		block := make([]byte, blockSize)
		if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
			return nil, 0, fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
		}
		blockEntries, parent, err := parseDirentBlock(block, dirInode)
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, blockEntries...)
		if parent != 0 {
			parentInode = parent
		}
	}
	if parentInode == 0 {
		return nil, 0, fmt.Errorf("directory inode %d has no .. entry", dirInode)
	}
	return entries, parentInode, nil
}

// emitHtree rebuilds dirInode (whose loaded inode is passed in) as a valid depth-1
// htree containing exactly entries (which must exclude "." and ".."). It computes
// the leaf layout first and returns errHtreeNotIndexable WITHOUT side effects if the
// directory cannot be indexed depth-1 — an empty set, an unsupported hash version,
// an oversized same-hash group, more than dxRootLimit leaves, or not enough free
// space to grow the index. Otherwise it reconciles the directory to its exact new
// size (logical blocks 0..K), writes the dx_root and leaves, and sets EXT4_INDEX_FL,
// i_size and i_blocks.
//
// version is the base hash version stored in dx_root_info (e.g. hashVersionHalfMD4);
// names are hashed with the builder's seed and signedness (b.hashSeed/b.signedHash),
// the latter conveyed on disk separately via the superblock's signedness flag.
func (b *builder) emitHtree(dirInode uint32, inode *inode, parentInode uint32, entries []dirEntry, version uint8) error {
	if len(entries) == 0 {
		return errHtreeNotIndexable
	}

	effVer := effectiveHashVersion(version, !b.signedHash)
	if !hashVersionSupported(effVer) {
		return errHtreeNotIndexable // we only hash with half_md4 — leave it linear (#3)
	}

	// --- Compute phase (no side effects) ---
	leaves, err := packHtreeLeaves(hashAndSortEntries(entries, b.hashSeed, effVer), blockSize)
	if err != nil {
		return err // errHtreeNotIndexable: an oversized same-hash group
	}
	if len(leaves) > dxRootLimit {
		return errHtreeNotIndexable
	}

	// Capacity guard: if indexing would have to grow the directory (its current
	// contiguous run cannot be reused in place) but the image is out of free blocks,
	// leave it linear rather than aborting Save on ENOSPC (#1/#2). Checked here, in
	// the side-effect-free compute phase, so the directory is never partially rebuilt.
	total := uint32(len(leaves) + 1)
	if _, count, ok := singleExtentRun(inode); (!ok || count < total) && !b.canAllocate(total) {
		return errHtreeNotIndexable
	}

	// --- Commit phase ---
	return b.commitHtreeLayout(dirInode, inode, parentInode, version, leaves)
}

// hashAndSortEntries computes each entry's (major, minor) hash and returns them
// sorted by (major, minor, name) — the canonical order the leaf packer relies on.
func hashAndSortEntries(entries []dirEntry, seed [4]uint32, effVer uint8) []hashedEntry {
	hes := make([]hashedEntry, len(entries))
	for i, e := range entries {
		maj, mnr := ext4Dirhash(e.Name, seed, effVer)
		hes[i] = hashedEntry{entry: e, major: maj, minor: mnr}
	}
	sort.Slice(hes, func(i, j int) bool {
		if hes[i].major != hes[j].major {
			return hes[i].major < hes[j].major
		}
		if hes[i].minor != hes[j].minor {
			return hes[i].minor < hes[j].minor
		}
		return bytes.Compare(hes[i].entry.Name, hes[j].entry.Name) < 0
	})
	return hes
}

// commitHtreeLayout reconciles dirInode (loaded inode passed in) to the planned
// leaves: it secures K+1 blocks for logical 0..K (reusing the directory's current
// run in place where it fits, else allocating fresh and freeing the old), writes the
// leaves and dx_root, and updates the inode (EXT4_INDEX_FL, i_size, i_blocks).
func (b *builder) commitHtreeLayout(dirInode uint32, inode *inode, parentInode uint32, version uint8, leaves []htreeLeafPlan) error {
	total := uint32(len(leaves) + 1) // dx_root + K leaves
	blocks, err := b.reconcileDirBlocks(inode, total)
	if err != nil {
		return err
	}

	// Write leaves (logical blocks 1..K -> physical blocks[1..K]).
	for i, leaf := range leaves {
		if err := b.writeDirBlock(blocks[i+1], dirInode, inode.Generation, leaf.entries); err != nil {
			return fmt.Errorf("failed to write htree leaf %d: %w", i, err)
		}
	}

	// Write the dx_root (logical block 0 -> physical blocks[0]).
	if err := b.writeDxRoot(blocks[0], dirInode, parentInode, version, leaves); err != nil {
		return err
	}

	inode.Flags |= inodeFlagIndex
	inode.SizeLo = total * blockSize
	inode.SizeHi = 0
	if err := b.writeInode(dirInode, inode); err != nil {
		return fmt.Errorf("failed to write htree directory inode: %w", err)
	}

	return nil
}

// reconcileDirBlocks resizes a directory to exactly total blocks and maps them as
// logical 0..total-1, recomputing i_blocks (data blocks plus any preserved xattr
// block, with the extent writer adding any extent-tree metadata it allocates). The
// block swap is atomic: the directory's current blocks are never freed until the new
// layout is secured (see atomicDirBlockSwap), so an allocation failure leaves the
// directory exactly as it was. It returns the mapped blocks. Shared by the htree
// emit and flatten paths, which only differ in what they write into the blocks.
func (b *builder) reconcileDirBlocks(inode *inode, total uint32) ([]uint32, error) {
	blocks, err := b.atomicDirBlockSwap(inode, total)
	if err != nil {
		return nil, err
	}

	b.initExtentHeader(inode)
	inode.BlocksLo = total * (blockSize / 512)
	if inode.FileACLLo != 0 {
		inode.BlocksLo += blockSize / 512
	}
	if err := b.setExtentMultiple(inode, blocks); err != nil {
		return nil, fmt.Errorf("failed to map directory blocks: %w", err)
	}
	return blocks, nil
}

// atomicDirBlockSwap returns the total physical blocks a directory should occupy,
// performing the swap without ever leaving the inode pointing at freed blocks.
//
// In-place reuse: a directory stored as a single contiguous extent (every
// own-emitted directory is) that already spans at least total blocks keeps its first
// total blocks and frees only the surplus tail. No allocation happens, so this can
// never fail mid-swap, the directory never relocates, and the bytes stay identical
// to the previous best-fit-reuse behavior — which keeps re-emit and denser-repack
// byte-stable (#8). A single contiguous extent has no extent-tree metadata, so
// freeing the tail leaks nothing.
//
// Otherwise (growth, or a multi-extent/tree directory): allocate the new blocks
// FIRST, then free the old ones. On allocation failure nothing has been freed and
// the inode still references its original blocks, leaving the directory valid (#1).
// emit may relocate the directory here; the old blocks return to the free pool and
// free counts stay correct.
func (b *builder) atomicDirBlockSwap(inode *inode, total uint32) ([]uint32, error) {
	if start, count, ok := singleExtentRun(inode); ok && count >= total {
		if count > total {
			if err := b.freeBlockRun(start+total, count-total); err != nil {
				return nil, fmt.Errorf("failed to free surplus directory blocks: %w", err)
			}
		}
		blocks := make([]uint32, total)
		for i := range blocks {
			blocks[i] = start + uint32(i)
		}
		return blocks, nil
	}

	blocks, err := b.allocateBlocks(total)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate directory blocks: %w", err)
	}
	if err := b.freeInodeExtentRuns(inode); err != nil {
		return nil, fmt.Errorf("failed to free directory blocks: %w", err)
	}
	return blocks, nil
}

// singleExtentRun reports whether the inode maps its data as exactly one contiguous
// extent stored inline (depth 0, one entry) and, if so, returns that run's physical
// start and block count. This is the shape of every htree directory we emit, so it
// identifies the directories eligible for in-place block reuse.
func singleExtentRun(inode *inode) (start, count uint32, ok bool) {
	if binary.LittleEndian.Uint16(inode.Block[0:2]) != extentMagic {
		return 0, 0, false
	}
	if binary.LittleEndian.Uint16(inode.Block[6:8]) != 0 { // depth must be 0
		return 0, 0, false
	}
	if binary.LittleEndian.Uint16(inode.Block[2:4]) != 1 { // exactly one extent
		return 0, 0, false
	}
	length := binary.LittleEndian.Uint16(inode.Block[16:18])
	physical := binary.LittleEndian.Uint32(inode.Block[20:24])
	return physical, uint32(length), true
}

// canAllocate reports whether allocateBlocks(n) would succeed WITHOUT allocating or
// touching the disk, mirroring its success condition: one free run large enough, or
// enough fresh blocks across the groups. emit uses it to leave a directory linear
// when indexing would have to grow it but the image is full, instead of aborting
// Save on ENOSPC.
func (b *builder) canAllocate(n uint32) bool {
	for _, r := range b.freeRuns {
		if r.count >= n {
			return true
		}
	}
	var fresh uint32
	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)
		fresh += gl.GroupStart + gl.BlocksInGroup - b.nextBlockPerGroup[g]
	}
	return fresh >= n
}

// writeDxRoot writes the depth-1 dx_root into logical block 0. Leaf i occupies
// logical block i+1, so dx_root slot 0 points at logical block 1 and dx_entry[i]
// (i>=1) carries leaf i's boundary hash and logical block.
func (b *builder) writeDxRoot(physBlock, dirInode, parentInode uint32, hashVersion uint8, leaves []htreeLeafPlan) error {
	block := make([]byte, blockSize)

	// Fake "." dirent (rec_len 12).
	binary.LittleEndian.PutUint32(block[0:], dirInode)
	binary.LittleEndian.PutUint16(block[4:], 12)
	block[6] = 1
	block[7] = ftDir
	block[8] = '.'

	// Fake ".." dirent; its rec_len covers the rest of the block so a linear
	// reader skips the index entirely.
	binary.LittleEndian.PutUint32(block[12:], parentInode)
	binary.LittleEndian.PutUint16(block[16:], blockSize-12)
	block[18] = 2
	block[19] = ftDir
	block[20] = '.'
	block[21] = '.'

	// dx_root_info: reserved_zero(4)=0, hash_version, info_length=8,
	// indirect_levels=0 (depth-1), unused_flags=0.
	binary.LittleEndian.PutUint32(block[dxRootInfoOffset:], 0)
	block[dxRootInfoOffset+4] = hashVersion
	block[dxRootInfoOffset+5] = 8
	block[dxRootInfoOffset+6] = 0
	block[dxRootInfoOffset+7] = 0

	// dx_countlimit (entry slot 0): limit, count, and the first leaf's block.
	binary.LittleEndian.PutUint16(block[dxCountLimitOffset:], dxRootLimit)
	binary.LittleEndian.PutUint16(block[dxCountLimitOffset+2:], uint16(len(leaves)))
	binary.LittleEndian.PutUint32(block[dxCountLimitOffset+4:], 1) // logical block of leaf 0

	// dx_entry[1..]: {boundary hash (low bit already cleared), logical block}.
	off := dxEntryArrayOffset
	for i := 1; i < len(leaves); i++ {
		binary.LittleEndian.PutUint32(block[off:], leaves[i].boundaryHash)
		binary.LittleEndian.PutUint32(block[off+4:], uint32(i+1))
		off += 8
	}

	if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(physBlock))); err != nil {
		return fmt.Errorf("failed to write dx_root block %d: %w", physBlock, err)
	}
	return nil
}

// prepareHtreeForMutation makes an htree directory safe for a linear insert. On
// the first insert into an EXT4_INDEX_FL directory it captures the dx_root hash
// version (before block 0 is overwritten), refuses depth-2 directories, flattens
// the directory to linear (clearing the index), and registers it to be re-indexed
// at finalize with the image's own hash parameters. Subsequent inserts see the
// cleared index flag and skip this path; the registry entry makes the re-index
// keep the captured (foreign) hash version and never set dirIndexUsed (decision 4/8).
func (b *builder) prepareHtreeForMutation(dirInode uint32) error {
	hashVersion, indirectLevels, err := b.dxRootInfo(dirInode)
	if err != nil {
		return err
	}
	if indirectLevels >= 1 {
		return fmt.Errorf("cannot modify depth-2 htree directory %d: not supported", dirInode)
	}
	// Refuse before the destructive flatten if we cannot re-index with this hash
	// (only half_md4 is supported), rather than failing at Save with the directory
	// already flattened and unindexed.
	if !hashVersionSupported(effectiveHashVersion(hashVersion, !b.signedHash)) {
		return fmt.Errorf("cannot modify htree directory %d: unsupported hash version %d", dirInode, hashVersion)
	}
	if err := b.flattenHtree(dirInode); err != nil {
		return err
	}
	b.reindexDirs[dirInode] = reindexInfo{foreign: true, hashVersion: hashVersion}
	return nil
}

// flattenHtree converts an htree directory back to an ordinary linear directory:
// it reads every real entry (and the real parent inode), reconciles the directory
// to linear dirent blocks, and clears EXT4_INDEX_FL. Used on the first mutation of
// a foreign htree directory so subsequent linear inserts are safe; the directory
// is re-indexed (if still warranted) at finalize.
func (b *builder) flattenHtree(dirInode uint32) error {
	entries, parentInode, err := b.readAllEntries(dirInode)
	if err != nil {
		return fmt.Errorf("failed to read htree directory for flatten: %w", err)
	}

	// Pack into linear blocks: block 0 leads with "." and "..".
	blocksEntries := [][]dirEntry{{
		{Inode: dirInode, Type: ftDir, Name: []byte(".")},
		{Inode: parentInode, Type: ftDir, Name: []byte("..")},
	}}
	curBytes := 12 + 12
	for _, e := range entries {
		rl := dirRecLen(len(e.Name))
		last := len(blocksEntries) - 1
		if curBytes+rl > blockSize {
			blocksEntries = append(blocksEntries, nil)
			last++
			curBytes = 0
		}
		blocksEntries[last] = append(blocksEntries[last], e)
		curBytes += rl
	}

	inode, err := b.readInode(dirInode)
	if err != nil {
		return fmt.Errorf("failed to read directory inode for flatten: %w", err)
	}

	total := uint32(len(blocksEntries))
	blocks, err := b.reconcileDirBlocks(inode, total)
	if err != nil {
		return err
	}

	for i, be := range blocksEntries {
		if err := b.writeDirBlock(blocks[i], dirInode, inode.Generation, be); err != nil {
			return fmt.Errorf("failed to write flattened directory block %d: %w", i, err)
		}
	}

	inode.Flags &^= inodeFlagIndex
	inode.SizeLo = total * blockSize
	inode.SizeHi = 0
	if err := b.writeInode(dirInode, inode); err != nil {
		return fmt.Errorf("failed to write flattened directory inode: %w", err)
	}

	return nil
}

// dxRootInfo reads an htree directory's dx_root_info (logical block 0): the base
// hash version (byte 28) and the indirect-levels depth marker (byte 30). The hash
// version must be captured before any mutation flattens block 0.
func (b *builder) dxRootInfo(dirInode uint32) (hashVersion, indirectLevels uint8, err error) {
	inode, err := b.readLiveDirInode(dirInode)
	if err != nil {
		return 0, 0, err
	}
	blocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return 0, 0, err
	}
	if len(blocks) == 0 {
		return 0, 0, fmt.Errorf("htree directory %d has no blocks", dirInode)
	}

	block := make([]byte, blockSize)
	if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blocks[0]))); err != nil {
		return 0, 0, fmt.Errorf("failed to read dx_root block: %w", err)
	}
	return block[dxRootInfoOffset+4], block[dxRootInfoOffset+6], nil
}
