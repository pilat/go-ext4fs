package ext4fs

import (
	"encoding/binary"
	"fmt"
)

// dirUsableEnd returns the byte offset at which real directory entries must stop.
// With metadata_csum the trailing 12 bytes are reserved for the ext4_dir_entry_tail,
// so the last real entry's rec_len ends at blockSize-12; without it entries fill the
// whole block.
func (b *builder) dirUsableEnd() int {
	if b.csumEnabled {
		return blockSize - dirEntryTailSize
	}

	return blockSize
}

// setDirTail stamps the ext4_dir_entry_tail into the last 12 bytes of a directory
// block and fills its det_checksum. It masquerades as a deleted entry (inode 0,
// rec_len 12, name_len 0, file_type 0xDE) so the linear readers skip it. The caller
// must have laid out real entries so the last one's rec_len ends at
// blockSize-dirEntryTailSize. Only called when csumEnabled.
func (b *builder) setDirTail(block []byte, dirInode, dirGen uint32) {
	off := blockSize - dirEntryTailSize

	binary.LittleEndian.PutUint32(block[off:], 0)                  // det_reserved_zero1 (inode)
	binary.LittleEndian.PutUint16(block[off+4:], dirEntryTailSize) // det_rec_len
	block[off+6] = 0                                               // det_reserved_zero2 (name_len)
	block[off+7] = dirEntryTailType                                // det_reserved_ft

	csum := dirBlockCsum(b.csumSeed, dirInode, dirGen, block)
	binary.LittleEndian.PutUint32(block[blockSize-4:], csum)
}

// writeDirBlock writes a block containing directory entries to disk.
// Directory entries are packed into the block with proper record length calculations
// to ensure correct parsing. The block becomes part of the directory's data extent.
// dirInode and dirGen identify the owning directory; with metadata_csum they sign the
// block's ext4_dir_entry_tail checksum (ignored when checksums are off).
func (b *builder) writeDirBlock(blockNum, dirInode, dirGen uint32, entries []dirEntry) error {
	block := make([]byte, blockSize)
	offset := 0
	usableEnd := b.dirUsableEnd()

	for i, entry := range entries {
		nameLen := len(entry.Name)

		recLen := dirRecLen(nameLen)

		if i == len(entries)-1 {
			recLen = usableEnd - offset
		}

		binary.LittleEndian.PutUint32(block[offset:], entry.Inode)
		binary.LittleEndian.PutUint16(block[offset+4:], uint16(recLen))
		block[offset+6] = uint8(nameLen)
		block[offset+7] = entry.Type
		copy(block[offset+8:], entry.Name)

		offset += recLen
	}

	if b.csumEnabled {
		b.setDirTail(block, dirInode, dirGen)
	}

	if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
		return fmt.Errorf("failed to write directory block %d: %w", blockNum, err)
	}

	return nil
}

// addDirEntry adds a new directory entry to the specified directory.
// Searches existing directory blocks for space, or allocates new blocks if needed.
// Updates the directory's size and block allocation as entries are added.
func (b *builder) addDirEntry(dirInode uint32, entry dirEntry) error {
	inode, err := b.readLiveDirInode(dirInode)
	if err != nil {
		return fmt.Errorf("failed to read directory inode: %w", err)
	}

	// htree guard: a linear insert into a hash-indexed directory would overwrite
	// its dx_root index (the corruption this whole feature fixes). With metadata_csum
	// we cannot maintain the index's dx_tail checksums, so we refuse rather than
	// flatten; otherwise, on the first such insert, flatten the directory to linear
	// and queue it for re-index at finalize, then fall through to the linear insert.
	if inode.Flags&inodeFlagIndex != 0 {
		if b.csumEnabled {
			return csumUnsupported("htree directory")
		}
		if err := b.prepareHtreeForMutation(dirInode); err != nil {
			return err
		}
		inode, err = b.readLiveDirInode(dirInode)
		if err != nil {
			return fmt.Errorf("failed to re-read flattened directory inode: %w", err)
		}
	}

	dirGen := inode.Generation

	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return fmt.Errorf("failed to get directory blocks: %w", err)
	}

	newRecLen := dirRecLen(len(entry.Name))

	for _, blockNum := range dataBlocks {
		if success, err := b.tryAddEntryToBlock(blockNum, dirInode, dirGen, entry, newRecLen); err != nil {
			return fmt.Errorf("failed to add entry to directory block %d: %w", blockNum, err)
		} else if success {
			return nil
		}
	}

	return b.appendEntryInNewBlock(dirInode, dirGen, entry)
}

// appendEntryInNewBlock grows the directory by one data block and writes entry as
// its sole record, then updates the inode's size and block count. dirGen stamps the
// metadata_csum directory tail. A mutation rejected after allocation (e.g. the
// metadata_csum external-extent guard) rolls the new block back so none is orphaned.
func (b *builder) appendEntryInNewBlock(dirInode, dirGen uint32, entry dirEntry) error {
	newBlock, err := b.allocateBlock()
	if err != nil {
		return err
	}

	if err := b.addBlockToInode(dirInode, newBlock); err != nil {
		if freeErr := b.freeBlockRun(newBlock, 1); freeErr != nil {
			return fmt.Errorf("%w (allocation rollback failed: %v)", err, freeErr)
		}
		return err
	}

	block := make([]byte, blockSize)
	binary.LittleEndian.PutUint32(block[0:], entry.Inode)
	binary.LittleEndian.PutUint16(block[4:], uint16(b.dirUsableEnd()))
	block[6] = uint8(len(entry.Name))
	block[7] = entry.Type
	copy(block[8:], entry.Name)

	if b.csumEnabled {
		b.setDirTail(block, dirInode, dirGen)
	}

	if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(newBlock))); err != nil {
		return fmt.Errorf("failed to write directory block: %w", err)
	}

	inode, err := b.readInode(dirInode)
	if err != nil {
		return fmt.Errorf("failed to re-read directory inode: %w", err)
	}

	inode.SizeLo += blockSize
	inode.BlocksLo += blockSize / 512
	if err := b.writeInode(dirInode, inode); err != nil {
		return fmt.Errorf("failed to update directory inode: %w", err)
	}

	return nil
}

// tryAddEntryToBlock attempts to add a directory entry to an existing directory block.
// Returns true if the entry fits in the available space, false if the block is full.
// Calculates proper record lengths to maintain directory entry structure integrity.
func (b *builder) tryAddEntryToBlock(blockNum, dirInode, dirGen uint32, entry dirEntry, newRecLen int) (bool, error) {
	block := make([]byte, blockSize)
	if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
		return false, fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
	}

	// With metadata_csum the trailing 12 bytes are the ext4_dir_entry_tail; the scan
	// stops at it, the new last entry's rec_len ends there, and the tail's
	// det_checksum is rewritten below.
	usableEnd := b.dirUsableEnd()

	offset := 0
	lastOffset := 0

	for offset < usableEnd {
		// Validate the record before walking past it, matching walkDirentBlock;
		// the in-place mutation below cannot delegate to it.
		if offset+8 > blockSize {
			return false, fmt.Errorf("directory block %d: truncated dirent at offset %d", blockNum, offset)
		}
		recLen := int(binary.LittleEndian.Uint16(block[offset+4:]))
		if recLen == 0 {
			break
		}
		if recLen < 8 || recLen%4 != 0 || offset+recLen > blockSize {
			return false, fmt.Errorf("directory block %d: invalid rec_len %d at offset %d", blockNum, recLen, offset)
		}

		lastOffset = offset
		offset += recLen
	}

	lastActualSize := dirRecLen(int(block[lastOffset+6]))

	lastRecLen := int(binary.LittleEndian.Uint16(block[lastOffset+4:]))

	// lastRecLen extends to usableEnd, so spaceAvailable already excludes the
	// reserved tail; no separate reservation is needed in the block-full math.
	spaceAvailable := lastRecLen - lastActualSize
	if spaceAvailable < newRecLen {
		return false, nil
	}

	binary.LittleEndian.PutUint16(block[lastOffset+4:], uint16(lastActualSize))

	newOffset := lastOffset + lastActualSize
	remaining := usableEnd - newOffset

	binary.LittleEndian.PutUint32(block[newOffset:], entry.Inode)
	binary.LittleEndian.PutUint16(block[newOffset+4:], uint16(remaining))
	block[newOffset+6] = uint8(len(entry.Name))
	block[newOffset+7] = entry.Type
	copy(block[newOffset+8:], entry.Name)

	if b.csumEnabled {
		b.setDirTail(block, dirInode, dirGen)
	}

	if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
		return false, fmt.Errorf("failed to write directory block %d: %w", blockNum, err)
	}

	return true, nil
}

// removeDirEntry removes a directory entry with the specified name from the directory.
// The entry is removed by expanding the previous entry's rec_len to absorb the deleted entry,
// or by setting inode=0 if it's the first entry in a block.
// Returns an error if the entry is not found.
func (b *builder) removeDirEntry(dirInode uint32, name string) error {
	inode, err := b.readInode(dirInode)
	if err != nil {
		return fmt.Errorf("failed to read directory inode for entry removal: %w", err)
	}

	// With metadata_csum we cannot maintain an htree directory's dx_tail checksums,
	// so refuse to mutate an indexed directory rather than corrupt its index.
	if b.csumEnabled && inode.Flags&inodeFlagIndex != 0 {
		return csumUnsupported("htree directory")
	}

	dirGen := inode.Generation
	usableEnd := b.dirUsableEnd()

	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return fmt.Errorf("failed to get directory blocks for entry removal: %w", err)
	}

	for _, blockNum := range dataBlocks {
		block := make([]byte, blockSize)
		if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
			return fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
		}

		found, err := removeEntryFromBlock(block, name, usableEnd)
		if err != nil {
			return fmt.Errorf("directory inode %d, block %d: %w", dirInode, blockNum, err)
		}
		if !found {
			continue
		}

		if b.csumEnabled {
			b.setDirTail(block, dirInode, dirGen)
		}

		if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
			return fmt.Errorf("failed to write directory block %d: %w", blockNum, err)
		}

		return nil
	}

	return fmt.Errorf("entry %q not found in directory", name)
}

// removeEntryFromBlock scans a single directory data block for name and, if
// present, removes the entry in place — zeroing the first record's inode or
// extending the previous record's rec_len to absorb it — and returns true.
// usableEnd excludes any metadata_csum tail. Each record is validated like
// walkDirentBlock before it is walked, since the in-place mutation cannot delegate.
func removeEntryFromBlock(block []byte, name string, usableEnd int) (bool, error) {
	offset := 0
	prevOffset := -1

	for offset < usableEnd {
		if offset+8 > blockSize {
			return false, fmt.Errorf("truncated dirent at offset %d", offset)
		}
		recLen := int(binary.LittleEndian.Uint16(block[offset+4:]))
		if recLen == 0 {
			break
		}
		if recLen < 8 || recLen%4 != 0 || offset+recLen > blockSize {
			return false, fmt.Errorf("invalid rec_len %d at offset %d", recLen, offset)
		}

		nameLen := int(block[offset+6])
		if nameLen > recLen-8 {
			return false, fmt.Errorf("invalid name_len %d at offset %d", nameLen, offset)
		}

		if string(block[offset+8:offset+8+nameLen]) == name {
			if prevOffset < 0 {
				// First entry in block: set inode to 0 to mark as unused.
				binary.LittleEndian.PutUint32(block[offset:], 0)
			} else {
				// Not first entry: expand previous entry's rec_len. The removed
				// entry ends at or before usableEnd, so this never swallows the tail.
				prevRecLen := binary.LittleEndian.Uint16(block[prevOffset+4:])
				binary.LittleEndian.PutUint16(block[prevOffset+4:], prevRecLen+uint16(recLen))
			}
			return true, nil
		}

		prevOffset = offset
		offset += recLen
	}

	return false, nil
}

// readAllEntries enumerates every real entry of a directory and recovers the
// parent inode from its ".." record. It is the htree-aware counterpart to
// listDirEntries used by the htree rebuild: the returned entries exclude "." and
// ".." (emit re-synthesizes those into the dx_root, flatten into block 0), while
// the parent inode — which listDirEntries discards — is returned separately
// because a rebuild must preserve it verbatim.
//
// Like listDirEntries it brute-scans every mapped data block, which is correct at
// any htree depth: in a dx_root the ".." record's rec_len runs to end-of-block, so
// a linear walk never reaches the index; dx_node index blocks begin with an
// inode=0 filler whose rec_len spans the block, so they contribute nothing; leaf
// blocks are ordinary dirent blocks. Hardlinks and file types pass through
// verbatim (each name is enumerated independently).
func (b *builder) readAllEntries(dirInode uint32) ([]dirEntry, uint32, error) {
	inode, err := b.readLiveDirInode(dirInode)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read directory inode: %w", err)
	}

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

// dirRecord is one live (inode != 0) directory record yielded by walkDirentBlock.
// name aliases the underlying block buffer; a caller that retains it must copy.
type dirRecord struct {
	offset   int
	recLen   int
	inode    uint32
	nameLen  int
	fileType uint8
	name     []byte
}

// walkDirentBlock is the single validated scanner for a directory data block. It
// follows the rec_len chain, stops at the terminating zero rec_len, and invokes
// fn for every live record. It applies the same bounds checks for every caller —
// rejecting a truncated header, a rec_len that is too small, misaligned, or runs
// past the block end, and a name_len that overflows its record — so no scanner
// ever slices a malformed on-disk dirent out of bounds. fn may return an error to
// abort the walk.
func walkDirentBlock(block []byte, fn func(rec dirRecord) error) error {
	for offset := 0; offset < blockSize; {
		if offset+8 > blockSize {
			return fmt.Errorf("truncated dirent at offset %d", offset)
		}
		recLen := int(binary.LittleEndian.Uint16(block[offset+4:]))
		if recLen == 0 {
			break
		}
		if recLen < 8 || recLen%4 != 0 || offset+recLen > blockSize {
			return fmt.Errorf("invalid rec_len %d at offset %d", recLen, offset)
		}

		if entryInode := binary.LittleEndian.Uint32(block[offset:]); entryInode != 0 {
			nameLen := int(block[offset+6])
			if nameLen > recLen-8 {
				return fmt.Errorf("invalid name_len %d at offset %d", nameLen, offset)
			}
			if err := fn(dirRecord{
				offset:   offset,
				recLen:   recLen,
				inode:    entryInode,
				nameLen:  nameLen,
				fileType: block[offset+7],
				name:     block[offset+8 : offset+8+nameLen],
			}); err != nil {
				return err
			}
		}

		offset += recLen
	}
	return nil
}

// parseDirentBlock returns the real entries in a directory block and the parent
// inode if the block carries a ".." record (0 otherwise). It walks the block
// through walkDirentBlock, so a malformed on-disk dirent surfaces as an error
// rather than an out-of-bounds slice.
func parseDirentBlock(block []byte, dirInode uint32) ([]dirEntry, uint32, error) {
	var (
		entries     []dirEntry
		parentInode uint32
	)
	err := walkDirentBlock(block, func(rec dirRecord) error {
		switch name := string(rec.name); name {
		case ".":
			// The directory's own inode; callers re-synthesize it.
		case "..":
			parentInode = rec.inode
		default:
			entries = append(entries, dirEntry{Inode: rec.inode, Type: rec.fileType, Name: []byte(name)})
		}
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("directory inode %d: %w", dirInode, err)
	}
	return entries, parentInode, nil
}

// findEntry searches for a directory entry with the specified name.
// Returns the inode number if found, or 0 if the entry doesn't exist.
// Used to check for existing files before creation or overwriting.
func (b *builder) findEntry(dirInode uint32, name string) (uint32, error) {
	inode, err := b.readLiveDirInode(dirInode)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory inode for entry search: %w", err)
	}

	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return 0, fmt.Errorf("failed to get directory blocks for entry search: %w", err)
	}

	for _, blockNum := range dataBlocks {
		block := make([]byte, blockSize)
		if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
			return 0, fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
		}

		var found uint32
		err := walkDirentBlock(block, func(rec dirRecord) error {
			if found == 0 && string(rec.name) == name {
				found = rec.inode
			}
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("directory inode %d, block %d: %w", dirInode, blockNum, err)
		}
		if found != 0 {
			return found, nil
		}
	}

	return 0, nil
}

// listDirEntries returns all directory entries in the specified directory.
// Skips entries with inode=0 (deleted entries) and "." / ".." entries.
// Returns a slice of dirEntry containing name, inode, and type for each entry.
func (b *builder) listDirEntries(dirInode uint32) ([]dirEntry, error) {
	inode, err := b.readInode(dirInode)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory inode: %w", err)
	}

	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return nil, fmt.Errorf("failed to get directory blocks: %w", err)
	}

	var entries []dirEntry

	for _, blockNum := range dataBlocks {
		block := make([]byte, blockSize)
		if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
			return nil, fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
		}

		err := walkDirentBlock(block, func(rec dirRecord) error {
			// Skip "." and ".."
			if name := string(rec.name); name != "." && name != ".." {
				entries = append(entries, dirEntry{Inode: rec.inode, Type: rec.fileType, Name: []byte(name)})
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("directory inode %d, block %d: %w", dirInode, blockNum, err)
		}
	}

	return entries, nil
}
