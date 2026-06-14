package ext4fs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertFreeBlockCountsMatchBitmaps checks that every group descriptor's free
// block count and the superblock total agree with the actual zero bits in the
// block bitmaps — the exact invariant e2fsck's "Free blocks count wrong" pass
// enforces.
func assertFreeBlockCountsMatchBitmaps(t *testing.T, img *Image) {
	t.Helper()

	b := img.builder
	gdtStart := b.layout.BlockOffset(b.layout.GetGroupLayout(0).GDTStart)

	var totalFree uint32

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		bm := make([]byte, blockSize)
		require.NoError(t, b.disk.readAt(bm, int64(b.layout.BlockOffset(gl.BlockBitmapBlock))))

		var groupFree uint32
		for i := uint32(0); i < gl.BlocksInGroup; i++ {
			if bm[i/8]&(1<<(i%8)) == 0 {
				groupFree++
			}
		}

		totalFree += groupFree

		gd := make([]byte, 32)
		require.NoError(t, b.disk.readAt(gd, int64(gdtStart+uint64(g*32))))
		assert.Equalf(t, uint16(groupFree), binary.LittleEndian.Uint16(gd[12:14]),
			"group %d free-block count must match its bitmap", g)
	}

	sb := make([]byte, 1024)
	require.NoError(t, b.disk.readAt(sb, superblockOffset))
	assert.Equal(t, totalFree, binary.LittleEndian.Uint32(sb[0x0C:0x10]),
		"superblock free-block count must match the bitmaps")
}

// TestReopenFreeCountWithHoles guards the free-block accounting across a reopen of
// an image that already has interior allocation holes (blocks free below the
// high-water mark). Before the fix, loadBlockBitmap recorded such holes for reuse
// but did not count them as freed, so calculateGroupStats treated them as used and
// every Save after reopen undercounted free blocks — which e2fsck rejects.
func TestReopenFreeCountWithHoles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holes.img")

	img, err := New(WithImagePath(path), WithSizeInMB(16))
	require.NoError(t, err)

	// Multi-block files advance the high-water mark; deleting alternates punches
	// holes that sit below it.
	for i := 0; i < 8; i++ {
		_, err := img.CreateFile(RootInode, fmt.Sprintf("f%d", i), bytes.Repeat([]byte("x"), 9000), 0644, 0, 0)
		require.NoError(t, err)
	}
	for i := 0; i < 8; i += 2 {
		require.NoError(t, img.Delete(RootInode, fmt.Sprintf("f%d", i)))
	}

	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	// Reopen; the holes now live in the on-disk bitmap. Saving with no changes must
	// already produce counts that match (pure reopen accounting).
	img, err = Open(WithExistingImagePath(path))
	require.NoError(t, err)
	require.NoError(t, img.Save())
	assertFreeBlockCountsMatchBitmaps(t, img)

	// Allocate into the holes (best-fit reuse) and append fresh data, then Save
	// again: the freed-hole bookkeeping must stay consistent as holes are consumed.
	for i := 0; i < 3; i++ {
		_, err := img.CreateFile(RootInode, fmt.Sprintf("g%d", i), bytes.Repeat([]byte("y"), 5000), 0644, 0, 0)
		require.NoError(t, err)
	}
	require.NoError(t, img.Save())
	assertFreeBlockCountsMatchBitmaps(t, img)

	require.NoError(t, img.Close())
}
