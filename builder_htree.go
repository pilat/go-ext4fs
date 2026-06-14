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

// errHtreeDepth1Exceeded is the typed sentinel emitHtree returns when the entry
// set cannot be represented as a depth-1 htree — either more than dxRootLimit
// leaves, or a single hash-collision group too large for one leaf. emitHtree
// guarantees NO side effects in this case (it is detected during the compute
// phase, before any block is freed or allocated), so the caller can safely leave
// the directory linear (own images) or refuse the mutation (foreign images).
var errHtreeDepth1Exceeded = errors.New("directory does not fit a depth-1 htree")

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
// errHtreeDepth1Exceeded if any single same-hash group exceeds one leaf.
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
			return nil, errHtreeDepth1Exceeded
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
// finalizeMetadata (so block allocation precedes free-count finalization,
// decision 6). For each registered directory whose live entries exceed one leaf
// block it attempts a depth-1 htree emit; directories that fit one leaf stay
// linear, and own directories that exceed the depth-1 bound fall back to linear
// (a foreign directory that no longer fits is an error). Only own-origin indexing
// sets dirIndexUsed, which in turn records the dir_index feature and signedness.
func (b *builder) emitHtreeDirs() error {
	inodes := make([]uint32, 0, len(b.reindexDirs))
	for ino := range b.reindexDirs {
		inodes = append(inodes, ino)
	}
	// Emit in a deterministic order (map iteration is randomized) so block
	// allocation — and thus the resulting image bytes — are reproducible.
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })

	for _, dirInode := range inodes {
		info := b.reindexDirs[dirInode]

		// The directory may have been deleted (or its inode reused) during the
		// session; skip anything that is no longer a live directory.
		allocated, err := b.isInodeAllocated(dirInode)
		if err != nil {
			return err
		}
		if !allocated {
			continue
		}
		ino, err := b.readInode(dirInode)
		if err != nil {
			return err
		}
		if ino.Mode&0xF000 != s_IFDIR {
			continue
		}

		entries, parent, err := b.readAllEntries(dirInode)
		if err != nil {
			return fmt.Errorf("failed to read directory %d for htree emit: %w", dirInode, err)
		}
		if liveEntryBytes(entries) <= blockSize {
			continue // fits one leaf block — stay linear
		}

		version := b.defHashVersion
		if info.foreign {
			version = info.hashVersion
		}

		err = b.emitHtree(dirInode, parent, entries, b.hashSeed, version, b.signedHash)
		if errors.Is(err, errHtreeDepth1Exceeded) {
			if info.foreign {
				return fmt.Errorf("foreign htree directory %d no longer fits a depth-1 htree", dirInode)
			}
			continue // own oversized directory stays linear (no regression)
		}
		if err != nil {
			return fmt.Errorf("failed to emit htree for directory %d: %w", dirInode, err)
		}

		if !info.foreign {
			b.dirIndexUsed = true
		}
	}

	return nil
}

// emitHtree rebuilds dirInode as a valid depth-1 htree containing exactly entries
// (which must exclude "." and ".."). It computes the leaf layout first and returns
// errHtreeDepth1Exceeded WITHOUT side effects if the set does not fit depth-1;
// otherwise it reconciles the directory to its exact new size by freeing all of
// its current data/extent blocks and reallocating K+1 contiguous-where-possible
// blocks (logical 0..K), writes the dx_root and leaves, and sets EXT4_INDEX_FL,
// i_size and i_blocks.
//
// version is the base hash version stored in dx_root_info (e.g. hashVersionHalfMD4);
// signed selects how names are hashed and is conveyed on disk separately via the
// superblock's signedness flag (the dx_root only records the base version).
func (b *builder) emitHtree(dirInode, parentInode uint32, entries []dirEntry, seed [4]uint32, version uint8, signed bool) error {
	if len(entries) == 0 {
		return errHtreeDepth1Exceeded
	}

	effVer := effectiveHashVersion(version, !signed)
	if !hashVersionSupported(effVer) {
		return fmt.Errorf("unsupported directory hash version %d", version)
	}

	// --- Compute phase (no side effects) ---
	leaves, err := packHtreeLeaves(hashAndSortEntries(entries, seed, effVer), blockSize)
	if err != nil {
		return err
	}
	if len(leaves) > dxRootLimit {
		return errHtreeDepth1Exceeded
	}

	// --- Commit phase ---
	return b.commitHtreeLayout(dirInode, parentInode, version, leaves)
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

// commitHtreeLayout reconciles dirInode to the planned leaves: it frees all of the
// directory's current data/extent blocks (preserving the xattr block), reallocates
// K+1 contiguous-where-possible blocks, writes the leaves and dx_root, and updates
// the inode (EXT4_INDEX_FL, i_size, i_blocks).
func (b *builder) commitHtreeLayout(dirInode, parentInode uint32, version uint8, leaves []htreeLeafPlan) error {
	inode, err := b.readInode(dirInode)
	if err != nil {
		return fmt.Errorf("failed to read directory inode for htree emit: %w", err)
	}

	total := uint32(len(leaves) + 1) // dx_root + K leaves
	blocks, err := b.reconcileDirBlocks(inode, total)
	if err != nil {
		return err
	}

	// Write leaves (logical blocks 1..K -> physical blocks[1..K]).
	for i, leaf := range leaves {
		if err := b.writeDirBlock(blocks[i+1], leaf.entries); err != nil {
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

// reconcileDirBlocks frees all of a directory's current data/extent blocks
// (preserving its xattr block) and reallocates exactly total contiguous-where-
// possible blocks, mapping them and recomputing i_blocks: data blocks plus the
// preserved xattr block, with the extent writer adding any extent-tree metadata it
// allocates. It returns the freshly mapped blocks. Shared by the htree emit and
// flatten paths, which only differ in what they write into the blocks.
func (b *builder) reconcileDirBlocks(inode *inode, total uint32) ([]uint32, error) {
	if err := b.freeInodeExtentRuns(inode); err != nil {
		return nil, fmt.Errorf("failed to free directory blocks: %w", err)
	}
	b.initExtentHeader(inode)

	blocks, err := b.allocateBlocks(total)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate directory blocks: %w", err)
	}

	inode.BlocksLo = total * (blockSize / 512)
	if inode.FileACLLo != 0 {
		inode.BlocksLo += blockSize / 512
	}
	if err := b.setExtentMultiple(inode, blocks); err != nil {
		return nil, fmt.Errorf("failed to map directory blocks: %w", err)
	}
	return blocks, nil
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
		if err := b.writeDirBlock(blocks[i], be); err != nil {
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
