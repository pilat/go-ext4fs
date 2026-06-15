package ext4fs

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Fail-loud guards (Task 4)
// =============================================================================

// TestChecksumGuardsRejectUnsupported verifies that operations whose metadata
// blocks bypass the checksum choke points fail loud on a metadata_csum image
// rather than silently emitting an e2fsck-rejectable image.
func TestChecksumGuardsRejectUnsupported(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(16), WithChecksum())
	require.NoError(t, err)

	fileInode, err := img.CreateFile(RootInode, "f", []byte("data"), 0644, 0, 0)
	require.NoError(t, err)

	t.Run("setXattr", func(t *testing.T) {
		err := img.SetXattr(fileInode, "user.k", []byte("v"))
		require.ErrorIs(t, err, errCsumUnsupported)
	})

	t.Run("removeXattr", func(t *testing.T) {
		err := img.RemoveXattr(fileInode, "user.k")
		require.ErrorIs(t, err, errCsumUnsupported)
	})

	t.Run("resize", func(t *testing.T) {
		err := img.Resize(img.Size() + blockSize)
		require.ErrorIs(t, err, errCsumUnsupported)
	})

	t.Run("externalExtentTree", func(t *testing.T) {
		var node inode
		err := img.builder.writeExtentTree(&node, make([]extent, 5))
		require.ErrorIs(t, err, errCsumUnsupported)
	})
}

// =============================================================================
// Self-consistency of our own checksummed output (Task 4)
// =============================================================================

// TestOurChecksumsSelfConsistent builds a checksummed image and re-derives every
// metadata checksum with the (independently mke2fs-validated) recipe helpers,
// asserting each matches what the writer stored. It catches wiring bugs — wrong
// offsets, a missing finalize step, the bitmap-before-bg_checksum ordering —
// without Docker, and
// independently of the e2fsck oracle in the e2e suite.
func TestOurChecksumsSelfConsistent(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(64), WithChecksum())
	require.NoError(t, err)

	sub, err := img.CreateDirectory(RootInode, "sub", 0755, 0, 0)
	require.NoError(t, err)
	_, err = img.CreateFile(sub, "a", []byte("hello world"), 0644, 0, 0)
	require.NoError(t, err)
	_, err = img.CreateFile(RootInode, "b", bytes.Repeat([]byte("x"), 9000), 0644, 0, 0)
	require.NoError(t, err)

	require.NoError(t, img.Save())

	// The New path derives the seed from the UUID and sets exactly these features.
	sb := make([]byte, 1024)
	require.NoError(t, img.builder.disk.readAt(sb, superblockOffset))
	assert.Equal(t, deriveCsumSeed(sb[0x68:0x78]), img.builder.csumSeed, "seed must equal crc32c(~0, uuid)")
	assert.Equal(t, uint32(roCompatSparseSuper|roCompatLargeFile|roCompatExtraIsize|roCompatMetadataCsum),
		binary.LittleEndian.Uint32(sb[0x64:]), "RO_COMPAT must carry metadata_csum")
	assert.Equal(t, uint8(checksumTypeCRC32C), sb[0x175], "checksum type")

	validateChecksumsSelfConsistent(t, img)
}

// validateChecksumsSelfConsistent re-derives every metadata_csum checksum in img
// from the current on-disk bytes and asserts each matches what the writer stored:
// the superblock, every group descriptor and its two bitmap checksums, every live
// inode, and the directory blocks of every live directory. It uses the same recipe
// helpers as the writer, so it catches wiring bugs (a missing recompute, a wrong
// offset, a bad finalize ordering) rather than recipe bugs (those are pinned
// against real mke2fs elsewhere). Inodes cleared in the bitmap (deleted) and
// unwritten reserved inodes are skipped, exactly as e2fsck treats them.
func validateChecksumsSelfConsistent(t *testing.T, img *Image) {
	t.Helper()

	b := img.builder
	seed := b.csumSeed

	sb := make([]byte, 1024)
	require.NoError(t, b.disk.readAt(sb, superblockOffset))
	assert.Equal(t, binary.LittleEndian.Uint32(sb[0x3FC:]), superblockCsum(sb), "superblock checksum")

	gdtStart := b.layout.BlockOffset(b.layout.GetGroupLayout(0).GDTStart)

	for g := uint32(0); g < b.layout.GroupCount; g++ {
		gl := b.layout.GetGroupLayout(g)

		gd := make([]byte, 32)
		require.NoError(t, b.disk.readAt(gd, int64(gdtStart+uint64(g*32))))

		blockBitmap := make([]byte, blockSize)
		require.NoError(t, b.disk.readAt(blockBitmap, int64(b.layout.BlockOffset(gl.BlockBitmapBlock))))
		assert.Equalf(t, binary.LittleEndian.Uint16(gd[0x18:]), bitmapCsum(seed, blockBitmap),
			"group %d block-bitmap checksum", g)

		inodeBitmap := make([]byte, blockSize)
		require.NoError(t, b.disk.readAt(inodeBitmap, int64(b.layout.BlockOffset(gl.InodeBitmapBlock))))
		assert.Equalf(t, binary.LittleEndian.Uint16(gd[0x1A:]), bitmapCsum(seed, inodeBitmap[:(inodesPerGroup+7)/8]),
			"group %d inode-bitmap checksum", g)

		assert.Equalf(t, binary.LittleEndian.Uint16(gd[0x1E:]), groupDescCsum(seed, g, gd),
			"group %d bg_checksum", g)

		for idx := uint32(0); idx < inodesPerGroup; idx++ {
			if inodeBitmap[idx/8]&(1<<(idx%8)) == 0 {
				continue // unallocated or deleted
			}

			n := g*inodesPerGroup + idx + 1

			raw := make([]byte, inodeSize)
			require.NoError(t, b.disk.readAt(raw, int64(b.layout.InodeOffset(n))))

			mode := binary.LittleEndian.Uint16(raw[0:])
			if mode == 0 {
				continue // reserved-but-unwritten inode (left zero, not checksummed)
			}

			lo, hi := inodeCsum(seed, n, raw)
			assert.Equalf(t, binary.LittleEndian.Uint16(raw[0x7C:]), lo, "inode %d checksum lo", n)
			assert.Equalf(t, binary.LittleEndian.Uint16(raw[0x82:]), hi, "inode %d checksum hi", n)

			if mode&0xF000 != s_IFDIR {
				continue
			}

			ino, err := b.readInode(n)
			require.NoError(t, err)

			blocks, err := b.getInodeBlocks(ino)
			require.NoError(t, err)

			for _, blk := range blocks {
				buf := make([]byte, blockSize)
				require.NoError(t, b.disk.readAt(buf, int64(b.layout.BlockOffset(blk))))
				assert.Equalf(t, binary.LittleEndian.Uint32(buf[blockSize-4:]),
					dirBlockCsum(seed, n, ino.Generation, buf), "dir %d block %d checksum", n, blk)
			}
		}
	}
}
