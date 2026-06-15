package ext4fs

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInodeAccountingScatteredMultiGroup reproduces finding #5: per-group free
// inodes, itable_unused, and the superblock free-inode count were derived from a
// single global nextInode cursor. That is wrong for multi-group foreign images
// whose inodes the kernel's Orlov allocator scatters into a higher group while a
// lower group still has free inodes above its local high-water. The inode bitmap
// is the authoritative record, so the counts must match a direct bitmap scan
// (what e2fsck computes) for both own (sequential) and scattered images. Block
// accounting uses a per-group cursor and is correct; it is not exercised here.
func TestInodeAccountingScatteredMultiGroup(t *testing.T) {
	// 256 MiB => 2 block groups (32768 blocks/group * 4096 B/block = 128 MiB).
	img, err := New(WithMemoryBackend(), WithSizeInMB(256))
	require.NoError(t, err)

	b := img.builder
	require.Equal(t, uint32(2), b.layout.GroupCount, "test needs a 2-group image")

	// Simulate the Orlov scatter: mark one inode used in group 1 and back it with
	// a minimal valid inode, leaving group 0's local high-water (lost+found,
	// inode 11) far below the now-global high-water that lives in group 1.
	const scattered = inodesPerGroup + 1 // first inode of group 1 (8193)
	require.NoError(t, b.setInodeBit(scattered))
	ino := b.makeFileInode(0644, 0, 0, 0)
	ino.LinksCount = 1
	require.NoError(t, b.writeInode(scattered, &ino))

	// Persist, then re-derive allocation state exactly as Open does: loadBitmaps
	// drives the global nextInode cursor up to the scattered high-water (8194),
	// which is what makes the buggy global-cursor accounting misreport group 0.
	require.NoError(t, img.Save())
	require.NoError(t, b.loadBitmaps())
	require.Equal(t, uint32(scattered+1), b.nextInode, "nextInode follows the scatter")

	// Re-finalize and read the counts back from disk.
	require.NoError(t, b.finalizeMetadata())

	var wantTotalFree uint32

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		used, wantItableUnused := trueInodeBitmapStats(t, b, g)
		gotFree, gotItableUnused := readGroupInodeCounts(t, b, g)

		assert.Equalf(t, uint16(inodesPerGroup)-used, gotFree, "group %d free inodes", g)
		assert.Equalf(t, wantItableUnused, gotItableUnused, "group %d itable_unused", g)

		wantTotalFree += uint32(inodesPerGroup) - uint32(used)
	}

	assert.Equal(t, wantTotalFree, readSuperblockFreeInodes(t, b), "superblock free inode count")
}

// trueInodeBitmapStats scans group g's inode bitmap directly for the
// authoritative used count and itable_unused (inodesPerGroup minus the highest
// set index plus one) — the same quantities e2fsck recomputes.
func trueInodeBitmapStats(t *testing.T, b *builder, g uint32) (used, itableUnused uint16) {
	t.Helper()

	gl := b.layout.GetGroupLayout(g)
	bm := make([]byte, blockSize)
	require.NoError(t, b.disk.readAt(bm, int64(b.layout.BlockOffset(gl.InodeBitmapBlock))))

	highest := -1
	for i := uint32(0); i < inodesPerGroup; i++ {
		if bm[i/8]&(1<<(i%8)) != 0 {
			highest = int(i)
			used++
		}
	}

	return used, uint16(inodesPerGroup - uint32(highest+1))
}

// readGroupInodeCounts reads the free-inode (offset 14) and itable_unused (offset
// 28) fields from group g's on-disk descriptor.
func readGroupInodeCounts(t *testing.T, b *builder, g uint32) (freeInodes, itableUnused uint16) {
	t.Helper()

	gdOffset := b.layout.BlockOffset(b.layout.GetGroupLayout(0).GDTStart) + uint64(g*32)
	gd := make([]byte, 32)
	require.NoError(t, b.disk.readAt(gd, int64(gdOffset)))

	return binary.LittleEndian.Uint16(gd[14:16]), binary.LittleEndian.Uint16(gd[28:30])
}

// readSuperblockFreeInodes reads s_free_inodes_count (superblock offset 0x10).
func readSuperblockFreeInodes(t *testing.T, b *builder) uint32 {
	t.Helper()

	sb := make([]byte, 1024)
	require.NoError(t, b.disk.readAt(sb, int64(b.layout.PartitionStart+superblockOffset)))

	return binary.LittleEndian.Uint32(sb[0x10:0x14])
}
