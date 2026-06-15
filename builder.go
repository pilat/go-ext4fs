package ext4fs

import (
	"fmt"
)

var DEBUG = false

// freeRun represents a contiguous range of free blocks
type freeRun struct {
	start uint32
	count uint32
}

type builder struct {
	disk   diskBackend
	layout *Layout
	debug  bool // Enable debug output

	label        string // Volume label written to the superblock (New path only)
	skipZeroInit bool   // Skip zeroing freshly-truncated inode tables (already zero)

	// Directory hashing (htree) parameters and state. The New path derives
	// hashSeed/signedHash; Open reads them from the foreign superblock. dirIndexUsed
	// records that an OWN-origin directory was indexed, which gates the dir_index
	// feature bit and signedness flag in updateSuperblocks (decision 8); it is never
	// set from the foreign reindex path.
	hashSeed       [4]uint32
	defHashVersion uint8
	signedHash     bool
	dirIndexUsed   bool

	// reindexDirs is the set of directories considered for htree indexing at
	// finalize, keyed by inode (auto-deduped). Own directories are registered by
	// createDirectory; foreign htree directories register on their first mutation.
	reindexDirs map[uint32]reindexInfo

	// Allocation state - per group
	nextBlockPerGroup   []uint32  // Next free block in each group
	freedBlocksPerGroup []uint32  // Blocks freed per group (for overwrites)
	freeRuns            []freeRun // Free block runs sorted by count (ascending) for best-fit
	nextInode           uint32    // Next free inode (global)
	freeInodeList       []uint32  // List of freed inodes available for reuse

	// Tracking
	usedDirsPerGroup []uint16 // Directory count per group
}

// reindexInfo records how a directory should be re-indexed at finalize. foreign
// distinguishes a directory that originated as a foreign htree (which keeps the
// image's hash version and must never set dirIndexUsed) from an own directory.
type reindexInfo struct {
	foreign     bool
	hashVersion uint8 // captured foreign dx_root hash version; unused for own dirs
}

// newBuilder creates a new Builder instance with initialized allocation state.
// It sets up per-group tracking for block and inode allocation, preparing
// the builder for filesystem construction operations.
func newBuilder(disk diskBackend, layout *Layout) *builder {
	b := &builder{
		disk:                disk,
		layout:              layout,
		debug:               DEBUG,
		label:               "ext4-go",
		nextBlockPerGroup:   make([]uint32, layout.GroupCount),
		freedBlocksPerGroup: make([]uint32, layout.GroupCount),
		freeRuns:            nil,
		nextInode:           firstNonResInode,
		freeInodeList:       make([]uint32, 0),
		usedDirsPerGroup:    make([]uint16, layout.GroupCount),
		defHashVersion:      hashVersionHalfMD4,
		signedHash:          true, // own images hash signed (decision 7); Open overrides
		reindexDirs:         make(map[uint32]reindexInfo),
	}

	// Initialize next free block for each group
	for g := uint32(0); g < layout.GroupCount; g++ {
		gl := layout.GetGroupLayout(g)
		b.nextBlockPerGroup[g] = gl.FirstDataBlock
	}

	return b
}

// loadBitmaps reads existing block and inode bitmaps from an opened ext4 image.
// It scans the bitmaps to determine which blocks and inodes are already allocated,
// enabling proper allocation state for modification operations.
//
// For each block group, it:
//   - Scans the block bitmap to find the first free block (sets nextBlockPerGroup)
//   - Scans the inode bitmap to find the highest allocated inode (sets nextInode)
//   - Reads the group descriptor to get the directory count (sets usedDirsPerGroup)
//
// This must be called after newBuilder when opening an existing image.
func (b *builder) loadBitmaps() error {
	highestInode := uint32(firstNonResInode - 1)

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		if err := b.loadBlockBitmap(g, gl); err != nil {
			return err
		}

		groupHighest, err := b.loadInodeBitmap(g, gl)
		if err != nil {
			return err
		}
		if groupHighest > highestInode {
			highestInode = groupHighest
		}

		if err := b.loadGroupDirCount(g); err != nil {
			return err
		}
	}

	b.nextInode = highestInode + 1

	if b.debug {
		fmt.Printf("Bitmaps loaded (next inode: %d)\n", b.nextInode)
	}

	return nil
}

// loadBlockBitmap scans a group's block bitmap to find allocation state and free holes.
func (b *builder) loadBlockBitmap(g uint32, gl GroupLayout) error {
	blockBitmap := make([]byte, blockSize)
	if err := b.disk.readAt(blockBitmap, int64(b.layout.BlockOffset(gl.BlockBitmapBlock))); err != nil {
		return fmt.Errorf("read block bitmap for group %d: %w", g, err)
	}

	// Find highest used block
	highestUsed := gl.FirstDataBlock - gl.GroupStart - 1
	dataStart := gl.FirstDataBlock - gl.GroupStart
	for i := dataStart; i < gl.BlocksInGroup; i++ {
		if blockBitmap[i/8]&(1<<(i%8)) != 0 {
			highestUsed = i
		}
	}
	b.nextBlockPerGroup[g] = gl.GroupStart + highestUsed + 1

	// Find free block runs (holes) below the high-water mark. They are reusable
	// (added to freeRuns) and also recorded as freed so the free-block count is
	// correct on a foreign image whose deleted files left holes. For our own
	// images (sequential allocation, no holes) this is zero, leaving the count
	// unchanged.
	var runStart, runCount, holes uint32
	for i := dataStart; i <= highestUsed; i++ {
		isFree := blockBitmap[i/8]&(1<<(i%8)) == 0
		if isFree {
			if runCount == 0 {
				runStart = gl.GroupStart + i
			}
			runCount++
		} else if runCount > 0 {
			b.addFreeRun(freeRun{start: runStart, count: runCount})
			holes += runCount
			runCount = 0
		}
	}
	if runCount > 0 {
		b.addFreeRun(freeRun{start: runStart, count: runCount})
		holes += runCount
	}
	b.freedBlocksPerGroup[g] = holes

	return nil
}

// loadInodeBitmap scans a group's inode bitmap and returns the highest allocated
// inode (0 if the group has none), used to seed the global nextInode cursor.
// Per-group inode usage is recomputed directly from the bitmap at finalize
// (calculateGroupStats), so no free-inode "holes" accounting is needed here.
func (b *builder) loadInodeBitmap(g uint32, gl GroupLayout) (highest uint32, err error) {
	inodeBitmap := make([]byte, blockSize)
	if err := b.disk.readAt(inodeBitmap, int64(b.layout.BlockOffset(gl.InodeBitmapBlock))); err != nil {
		return 0, fmt.Errorf("read inode bitmap for group %d: %w", g, err)
	}

	highestIndex := -1
	for i := uint32(0); i < b.layout.InodesPerGroup; i++ {
		if inodeBitmap[i/8]&(1<<(i%8)) != 0 {
			highestIndex = int(i)
		}
	}
	if highestIndex < 0 {
		return 0, nil
	}
	return g*b.layout.InodesPerGroup + uint32(highestIndex) + 1, nil
}

// loadGroupDirCount reads the directory count from a group descriptor.
func (b *builder) loadGroupDirCount(g uint32) error {
	gdtOffset := b.layout.BlockOffset(b.layout.GetGroupLayout(0).GDTStart) + uint64(g*32)
	gdData := make([]byte, 32)
	if err := b.disk.readAt(gdData, int64(gdtOffset)); err != nil {
		return fmt.Errorf("read group descriptor %d: %w", g, err)
	}
	b.usedDirsPerGroup[g] = uint16(gdData[16]) | uint16(gdData[17])<<8
	return nil
}

// prepareFilesystem initializes the complete ext4 filesystem structure.
// This includes writing the superblock, group descriptors, initializing
// bitmaps, zeroing inode tables, and creating essential directories like
// root and lost+found. This method must be called before any file operations.
func (b *builder) prepareFilesystem() error {
	if b.debug {
		fmt.Println(b.layout.String())
		fmt.Println()
	}

	if err := b.writeSuperblock(); err != nil {
		return err
	}

	if err := b.writeGroupDescriptors(); err != nil {
		return err
	}

	if err := b.initBitmaps(); err != nil {
		return err
	}

	if err := b.zeroInodeTables(0, b.layout.GroupCount); err != nil {
		return err
	}

	if err := b.createRootDirectory(); err != nil {
		return err
	}

	if err := b.createLostFound(); err != nil {
		return err
	}

	if DEBUG {
		fmt.Println("✓ Filesystem prepared successfully")
	}

	return nil
}
