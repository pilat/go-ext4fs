package ext4fs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// castagnoli is the CRC32C (Castagnoli) polynomial table backing every ext4
// metadata_csum checksum.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// errCsumUnsupported is the sentinel returned (wrapped) for operations that would
// write metadata blocks the checksum machinery does not cover yet — xattr blocks,
// external extent-tree leaves, htree nodes, the resize structural writers. Failing
// loud here prevents silently emitting an image e2fsck would reject. Callers and
// tests match it with errors.Is.
var errCsumUnsupported = errors.New("metadata_csum not yet supported")

// csumUnsupported wraps errCsumUnsupported, naming the offending capability.
func csumUnsupported(what string) error {
	return fmt.Errorf("%w with %s", errCsumUnsupported, what)
}

// crc32c computes ext2fs_crc32c_le(seed, p): CRC32C, bit-reflected, with an
// explicit running seed and NO final inversion. Go's crc32.Update inverts the
// crc on both entry and exit, so both inversions are cancelled here (^seed in,
// ^result out). A plain crc32.Update would return the bitwise complement of the
// value ext4 expects and every checksum would silently mismatch.
func crc32c(seed uint32, p []byte) uint32 {
	return ^crc32.Update(^seed, castagnoli, p)
}

// deriveCsumSeed computes the filesystem-wide checksum seed from the 16-byte
// superblock UUID: crc32c(0xFFFFFFFF, uuid). The group-descriptor, bitmap,
// inode, and directory checksums chain from this seed; the superblock checksum
// does not (the UUID is inside its own covered range).
func deriveCsumSeed(uuid []byte) uint32 {
	return crc32c(0xFFFFFFFF, uuid)
}

// superblockCsum computes the superblock checksum over sb[0:0x3FC] — every byte
// up to but excluding the s_checksum field. The seed is plain 0xFFFFFFFF, NOT
// the FS seed, because the UUID is already inside the covered bytes. sb must be
// the full 1024-byte superblock image.
func superblockCsum(sb []byte) uint32 {
	return crc32c(0xFFFFFFFF, sb[:0x3FC])
}

// groupDescCsum computes the metadata_csum group-descriptor checksum: fold the
// group number as le32 into the FS seed, then checksum the FULL descriptor with
// bg_checksum (offset 0x1E) zeroed. The result is truncated to 16 bits (the low
// half stored at bg_checksum; the high half would live in a 64-byte descriptor we
// do not use). desc must be the full 32-byte on-disk descriptor; it is not mutated.
//
// Coverage is the entire descriptor with its 2-byte checksum field zeroed —
// verified byte-for-byte against mke2fs output across multiple groups
// (csum_internal_test.go). Checksumming only desc[0:0x1E] yields a different,
// wrong value.
func groupDescCsum(fsSeed, group uint32, desc []byte) uint16 {
	buf := make([]byte, len(desc))
	copy(buf, desc)
	binary.LittleEndian.PutUint16(buf[0x1E:0x20], 0)

	var g [4]byte
	binary.LittleEndian.PutUint32(g[:], group)

	c := crc32c(fsSeed, g[:])
	c = crc32c(c, buf)

	return uint16(c & 0xFFFF)
}

// bitmapCsum computes a block- or inode-bitmap checksum: crc32c(fsSeed, bytes)
// truncated to 16 bits. The caller passes the exact on-disk byte range — the
// full 4096-byte block for the block bitmap, the used 1024-byte prefix for the
// inode bitmap.
func bitmapCsum(fsSeed uint32, bytes []byte) uint16 {
	return uint16(crc32c(fsSeed, bytes) & 0xFFFF)
}

// inodeCsum computes the 256-byte inode checksum. It folds the inode number as
// le32, then i_generation (read from offset 0x64 of the inode image) as le32,
// then checksums the full 256 bytes with BOTH checksum fields zeroed
// (i_checksum_lo @ 0x7C, i_checksum_hi @ 0x82). It returns the two 16-bit halves
// stored at those offsets. inodeBytes is not mutated.
//
// The high half is always written: this library and modern mke2fs both set
// i_extra_isize = 32 (≥ 4, the threshold above which i_checksum_hi is valid), so
// the assumption holds for every inode we read or write here.
func inodeCsum(fsSeed, inodeNum uint32, inodeBytes []byte) (lo, hi uint16) {
	buf := make([]byte, inodeSize)
	copy(buf, inodeBytes)

	binary.LittleEndian.PutUint16(buf[0x7C:0x7E], 0)
	binary.LittleEndian.PutUint16(buf[0x82:0x84], 0)

	generation := binary.LittleEndian.Uint32(buf[0x64:0x68])

	var num [4]byte
	binary.LittleEndian.PutUint32(num[:], inodeNum)

	c := crc32c(fsSeed, num[:])

	var gen [4]byte
	binary.LittleEndian.PutUint32(gen[:], generation)

	c = crc32c(c, gen[:])
	c = crc32c(c, buf)

	return uint16(c & 0xFFFF), uint16((c >> 16) & 0xFFFF)
}

// dirBlockCsum computes the directory-block checksum stored in the
// ext4_dir_entry_tail's det_checksum field. It folds the owning directory's
// inode number then its i_generation (both le32) into the FS seed, then
// checksums block[0:bs-12] (everything except the 12-byte tail). block must be
// the full blockSize image; the trailing tail bytes are excluded from coverage.
func dirBlockCsum(fsSeed, dirInode, dirGeneration uint32, block []byte) uint32 {
	var num [4]byte
	binary.LittleEndian.PutUint32(num[:], dirInode)

	c := crc32c(fsSeed, num[:])

	var gen [4]byte
	binary.LittleEndian.PutUint32(gen[:], dirGeneration)

	c = crc32c(c, gen[:])

	return crc32c(c, block[:len(block)-dirEntryTailSize])
}
