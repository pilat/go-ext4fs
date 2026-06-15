package ext4fs

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// corruptDir builds a memory image with directory "d" holding one real child
// entry, then returns the builder, the directory inode, and the physical block
// number of the directory's first (and only) data block so a test can surgically
// corrupt an on-disk dirent. Block 0 of "d" is laid out as:
//
//	offset  0: "."     rec_len 12
//	offset 12: ".."    rec_len 12
//	offset 24: "child" rec_len blockSize-24 (4072), name_len 5
func corruptDir(t *testing.T) (b *builder, dInode, blockNum uint32) {
	t.Helper()

	img, err := New(WithMemoryBackend(), WithSizeInMB(16))
	require.NoError(t, err)
	b = img.builder

	dInode, err = b.createDirectory(RootInode, "d", 0o755, 0, 0)
	require.NoError(t, err)

	_, err = b.createFile(dInode, "child", []byte("x"), 0o644, 0, 0)
	require.NoError(t, err)

	ino, err := b.readInode(dInode)
	require.NoError(t, err)
	blocks, err := b.getInodeBlocks(ino)
	require.NoError(t, err)
	require.Len(t, blocks, 1, "child must fit in the directory's first block")

	return b, dInode, blocks[0]
}

func readDirBlock(t *testing.T, b *builder, blockNum uint32) []byte {
	t.Helper()
	block := make([]byte, blockSize)
	require.NoError(t, b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))))
	return block
}

func writeDirBlock(t *testing.T, b *builder, blockNum uint32, block []byte) {
	t.Helper()
	require.NoError(t, b.disk.writeAt(block, int64(b.layout.BlockOffset(blockNum))))
}

// TestListDirEntriesRejectsOvershootRecLen is BUG (B), the silent-corruption
// case. Corrupting the ".." rec_len so it overshoots the block end makes the
// linear scan skip the real trailing "child" record: a non-empty directory is
// enumerated as empty. listDirEntries must report the malformed block as an error
// instead of silently under-enumerating.
func TestListDirEntriesRejectsOvershootRecLen(t *testing.T) {
	b, dInode, blockNum := corruptDir(t)

	block := readDirBlock(t, b, blockNum)
	// ".." sits at offset 12; its rec_len is at byte 16. Set it to span past the
	// block end so the walk jumps over "child" at offset 24.
	binary.LittleEndian.PutUint16(block[16:], uint16(blockSize))
	writeDirBlock(t, b, blockNum, block)

	_, err := b.listDirEntries(dInode)
	require.Error(t, err, "overshooting rec_len must be rejected, not silently under-enumerated")
}

// TestDeleteEntryDoesNotOrphanChildren proves the danger of BUG (B): with the
// ".." rec_len overshoot above, the empty-dir check sees zero entries, so the
// delete path removes a non-empty directory and frees its block, orphaning the
// real child. The delete must fail instead.
func TestDeleteEntryDoesNotOrphanChildren(t *testing.T) {
	b, _, blockNum := corruptDir(t)

	block := readDirBlock(t, b, blockNum)
	binary.LittleEndian.PutUint16(block[16:], uint16(blockSize))
	writeDirBlock(t, b, blockNum, block)

	err := b.deleteEntry(RootInode, "d")
	require.Error(t, err, "must refuse to delete a directory whose contents cannot be safely enumerated")
}

// TestDirScannersRejectWalkPastBlockEnd is BUG (A), the panic case. A rec_len
// that is not a multiple of 4 walks the scan offset into the last few bytes of
// the block; the next header read then slices out of bounds and panics. Every
// directory-block scanner must validate the record and return an error instead.
func TestDirScannersRejectWalkPastBlockEnd(t *testing.T) {
	corrupt := func(t *testing.T) (*builder, uint32, uint32) {
		b, dInode, blockNum := corruptDir(t)
		block := readDirBlock(t, b, blockNum)
		// "child" sits at offset 24; its rec_len is at byte 28. 4070 is not a
		// multiple of 4, so the offset advances to 4094 (past blockSize-4) and the
		// following header read panics in the unvalidated scanners.
		binary.LittleEndian.PutUint16(block[28:], 4070)
		writeDirBlock(t, b, blockNum, block)
		return b, dInode, blockNum
	}

	t.Run("listDirEntries", func(t *testing.T) {
		b, dInode, _ := corrupt(t)
		var err error
		require.NotPanics(t, func() { _, err = b.listDirEntries(dInode) })
		require.Error(t, err)
	})

	t.Run("findEntry", func(t *testing.T) {
		b, dInode, _ := corrupt(t)
		var err error
		require.NotPanics(t, func() { _, err = b.findEntry(dInode, "absent") })
		require.Error(t, err)
	})

	t.Run("removeDirEntry", func(t *testing.T) {
		b, dInode, _ := corrupt(t)
		var err error
		require.NotPanics(t, func() { err = b.removeDirEntry(dInode, "absent") })
		require.Error(t, err)
	})

	t.Run("tryAddEntryToBlock", func(t *testing.T) {
		b, _, blockNum := corrupt(t)
		entry := dirEntry{Inode: 11, Type: ftRegFile, Name: []byte("zz")}
		var err error
		require.NotPanics(t, func() { _, err = b.tryAddEntryToBlock(blockNum, entry, dirRecLen(2)) })
		require.Error(t, err)
	})
}
