package ext4fs

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dirFirstBlock returns the first directory data block of dirInode.
func dirFirstBlock(t *testing.T, img *Image, dirInode uint32) []byte {
	t.Helper()

	ino, err := img.builder.readInode(dirInode)
	require.NoError(t, err)

	blocks, err := img.builder.getInodeBlocks(ino)
	require.NoError(t, err)
	require.NotEmpty(t, blocks)

	buf := make([]byte, blockSize)
	require.NoError(t, img.backend.readAt(buf, int64(img.builder.layout.BlockOffset(blocks[0]))))

	return buf
}

// lastRealEntryEnd walks the real directory entries (stopping at usableEnd) and
// returns the byte offset where the last one ends.
func lastRealEntryEnd(block []byte, usableEnd int) int {
	offset := 0
	end := 0

	for offset < usableEnd {
		recLen := int(binary.LittleEndian.Uint16(block[offset+4:]))
		if recLen == 0 {
			break
		}

		end = offset + recLen
		offset += recLen
	}

	return end
}

// assertValidTail checks the ext4_dir_entry_tail layout and that its det_checksum
// matches the recipe for the owning directory.
func assertValidTail(t *testing.T, img *Image, block []byte, dirInode uint32) {
	t.Helper()

	off := blockSize - dirEntryTailSize
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(block[off:]), "tail inode must be 0")
	assert.Equal(t, uint16(dirEntryTailSize), binary.LittleEndian.Uint16(block[off+4:]), "tail rec_len must be 12")
	assert.Equal(t, uint8(0), block[off+6], "tail name_len must be 0")
	assert.Equal(t, uint8(dirEntryTailType), block[off+7], "tail file_type must be 0xDE")

	// Real entries must stop exactly at the tail.
	assert.Equal(t, off, lastRealEntryEnd(block, off), "last real entry must end at blockSize-12")

	ino, err := img.builder.readInode(dirInode)
	require.NoError(t, err)

	want := dirBlockCsum(img.builder.csumSeed, dirInode, ino.Generation, block)
	assert.Equal(t, want, binary.LittleEndian.Uint32(block[blockSize-4:]), "det_checksum must match recipe")
}

// TestDirTailWrittenWithChecksum verifies the root directory block carries a valid
// ext4_dir_entry_tail (correct layout + det_checksum) when WithChecksum is set.
func TestDirTailWrittenWithChecksum(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(8), WithChecksum())
	require.NoError(t, err)

	assertValidTail(t, img, dirFirstBlock(t, img, RootInode), RootInode)
}

// TestDirNoTailWithoutChecksum verifies the default path reserves no tail: the
// last real entry's rec_len runs to the full block and the sentinel file_type is
// absent.
func TestDirNoTailWithoutChecksum(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(8))
	require.NoError(t, err)

	block := dirFirstBlock(t, img, RootInode)
	assert.Equal(t, blockSize, lastRealEntryEnd(block, blockSize), "last entry must fill the whole block")
	assert.NotEqual(t, uint8(dirEntryTailType), block[blockSize-5], "no tail sentinel without checksum")
}

// TestDirEntriesPackIntoOneBlockWithChecksum guards the silent under-packing bug
// e2fsck does not catch: many small entries must share one block, not one each.
func TestDirEntriesPackIntoOneBlockWithChecksum(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(8), WithChecksum())
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := img.CreateFile(RootInode, fmt.Sprintf("f%02d", i), []byte("x"), 0644, 0, 0)
		require.NoError(t, err)
	}

	ino, err := img.builder.readInode(RootInode)
	require.NoError(t, err)

	blocks, err := img.builder.getInodeBlocks(ino)
	require.NoError(t, err)
	assert.Len(t, blocks, 1, "small entries must pack into a single block")

	// listDirEntries must enumerate exactly the real entries (10 files +
	// lost+found), skipping the tail and "."/"..".
	entries, err := img.builder.listDirEntries(RootInode)
	require.NoError(t, err)
	assert.Len(t, entries, 11)

	// findEntry must locate a real entry and not be fooled into matching the tail.
	got, err := img.builder.findEntry(RootInode, "f05")
	require.NoError(t, err)
	assert.NotZero(t, got)

	missing, err := img.builder.findEntry(RootInode, "nope")
	require.NoError(t, err)
	assert.Zero(t, missing)

	assertValidTail(t, img, dirFirstBlock(t, img, RootInode), RootInode)
}

// TestDirOverflowSecondBlockWithChecksum fills the root block past capacity and
// verifies a second block is allocated, every entry is still enumerable, and both
// blocks carry a valid tail.
func TestDirOverflowSecondBlockWithChecksum(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(16), WithChecksum())
	require.NoError(t, err)

	longName := strings.Repeat("a", 200) // rec_len 208 each; ~19 fill a 4096 block

	created := 0
	for {
		_, err := img.CreateFile(RootInode, fmt.Sprintf("%03d%s", created, longName), []byte("x"), 0644, 0, 0)
		require.NoError(t, err)
		created++

		ino, err := img.builder.readInode(RootInode)
		require.NoError(t, err)

		blocks, err := img.builder.getInodeBlocks(ino)
		require.NoError(t, err)

		if len(blocks) > 1 {
			// Every directory block must carry its own valid tail.
			for _, blk := range blocks {
				buf := make([]byte, blockSize)
				require.NoError(t, img.backend.readAt(buf, int64(img.builder.layout.BlockOffset(blk))))
				assertValidTail(t, img, buf, RootInode)
			}

			break
		}

		require.Less(t, created, 100, "directory never overflowed to a second block")
	}

	entries, err := img.builder.listDirEntries(RootInode)
	require.NoError(t, err)
	assert.Len(t, entries, created+1, "all files plus lost+found must remain enumerable")

	// The entry that triggered the overflow is the lone first entry of the second
	// block, so deleting it drives removeDirEntry's first-entry-in-block branch
	// (prevOffset < 0): it zeroes the entry's inode and must recompute the tail.
	overflowName := fmt.Sprintf("%03d%s", created-1, longName)
	require.NoError(t, img.Delete(RootInode, overflowName))

	ino, err := img.builder.readInode(RootInode)
	require.NoError(t, err)
	blocks, err := img.builder.getInodeBlocks(ino)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	second := make([]byte, blockSize)
	require.NoError(t, img.backend.readAt(second, int64(img.builder.layout.BlockOffset(blocks[1]))))
	assert.Zero(t, binary.LittleEndian.Uint32(second[0:]), "deleted first entry must have inode 0")
	assertValidTail(t, img, second, RootInode)

	entries, err = img.builder.listDirEntries(RootInode)
	require.NoError(t, err)
	assert.Len(t, entries, created, "deleted entry must drop from enumeration")
}
