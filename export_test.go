package ext4fs

import "encoding/binary"

// Test-only re-exports. This file is compiled only into the test binary (it is a
// _test.go file in package ext4fs), so it exposes internals to the external
// ext4fs_test package without widening the real public API.

// EmitHtreeForTest reads dirInode's current entries and rebuilds it as a depth-1
// htree using the builder's own hash params (as the own-images finalize path
// will), marking dirIndexUsed so Save records the dir_index feature and signedness
// flag. Returns the typed sentinel (errHtreeNotIndexable) unchanged when the
// directory cannot be indexed depth-1.
func (e *Image) EmitHtreeForTest(dirInode uint32) error {
	b := e.builder
	inode, err := b.readInode(dirInode)
	if err != nil {
		return err
	}
	entries, parent, err := b.readAllEntries(dirInode)
	if err != nil {
		return err
	}
	if err := b.emitHtree(dirInode, inode, parent, entries, b.defHashVersion); err != nil {
		return err
	}
	b.dirIndexUsed = true
	return nil
}

// FlattenHtreeForTest exposes flattenHtree.
func (e *Image) FlattenHtreeForTest(dirInode uint32) error {
	return e.builder.flattenHtree(dirInode)
}

// InodeFieldsForTest returns the inode fields htree tests assert on.
func (e *Image) InodeFieldsForTest(inodeNum uint32) (flags, sizeLo, blocksLo uint32, extentEntries, extentDepth uint16, err error) {
	ino, err := e.builder.readInode(inodeNum)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	flags = ino.Flags
	sizeLo = ino.SizeLo
	blocksLo = ino.BlocksLo
	extentEntries = binary.LittleEndian.Uint16(ino.Block[2:4])
	extentDepth = binary.LittleEndian.Uint16(ino.Block[6:8])
	return flags, sizeLo, blocksLo, extentEntries, extentDepth, nil
}

// DirBlocksForTest returns the physical data blocks of a directory inode.
func (e *Image) DirBlocksForTest(inodeNum uint32) ([]uint32, error) {
	ino, err := e.builder.readInode(inodeNum)
	if err != nil {
		return nil, err
	}
	return e.builder.getInodeBlocks(ino)
}

// ReadBlockForTest reads a physical block by number.
func (e *Image) ReadBlockForTest(physBlock uint32) ([]byte, error) {
	buf := make([]byte, blockSize)
	if err := e.builder.disk.readAt(buf, int64(e.builder.layout.BlockOffset(physBlock))); err != nil {
		return nil, err
	}
	return buf, nil
}

// DxRootLimitForTest is the no-csum dx_root limit (508).
const DxRootLimitForTest = dxRootLimit

// Ext4DirhashForTest exposes ext4Dirhash for the e2e de-risk tests, which must
// reproduce the on-disk dx_entry hashes of a real mke2fs htree directory.
func Ext4DirhashForTest(name []byte, seed [4]uint32, version uint8) (hash, minorHash uint32) {
	return ext4Dirhash(name, seed, version)
}

// EffectiveHashVersionForTest exposes effectiveHashVersion (signedness resolution).
func EffectiveHashVersionForTest(version uint8, unsigned bool) uint8 {
	return effectiveHashVersion(version, unsigned)
}

// FindEntryForTest exposes findEntry for tests that navigate a foreign image.
func (e *Image) FindEntryForTest(parent uint32, name string) (uint32, error) {
	return e.builder.findEntry(parent, name)
}

// ListNamesForTest returns the names listDirEntries enumerates (".", ".." excluded).
func (e *Image) ListNamesForTest(dirInode uint32) ([]string, error) {
	entries, err := e.builder.listDirEntries(dirInode)
	if err != nil {
		return nil, err
	}
	return entryNames(entries), nil
}

// ReadAllEntriesForTest exposes readAllEntries: the real entry names plus the
// recovered parent inode.
func (e *Image) ReadAllEntriesForTest(dirInode uint32) (names []string, parentInode uint32, err error) {
	entries, parent, err := e.builder.readAllEntries(dirInode)
	if err != nil {
		return nil, 0, err
	}
	return entryNames(entries), parent, nil
}

// InodeFlagsForTest returns an inode's i_flags (e.g. to assert EXT4_INDEX_FL).
func (e *Image) InodeFlagsForTest(inodeNum uint32) (uint32, error) {
	ino, err := e.builder.readInode(inodeNum)
	if err != nil {
		return 0, err
	}
	return ino.Flags, nil
}

func entryNames(entries []dirEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = string(e.Name)
	}
	return names
}

// On-disk hash constants surfaced for the e2e tests.
const (
	HashVersionHalfMD4ForTest         = hashVersionHalfMD4
	HashVersionHalfMD4UnsignedForTest = hashVersionHalfMD4Unsigned
	FlagsSignedHashForTest            = flagsSignedHash
	FlagsUnsignedHashForTest          = flagsUnsignedHash
	InodeFlagIndexForTest             = inodeFlagIndex
	CompatDirIndexForTest             = compatDirIndex
)
