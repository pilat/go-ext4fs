package ext4fs

import (
	"encoding/binary"
	"fmt"
)

// writeDirBlock writes a block containing directory entries to disk.
// Directory entries are packed into the block with proper record length calculations
// to ensure correct parsing. The block becomes part of the directory's data extent.
func (b *builder) writeDirBlock(blockNum uint32, entries []dirEntry) error {
	block := make([]byte, blockSize)
	offset := 0

	for i, entry := range entries {
		nameLen := len(entry.Name)

		recLen := dirRecLen(nameLen)

		if i == len(entries)-1 {
			recLen = blockSize - offset
		}

		binary.LittleEndian.PutUint32(block[offset:], entry.Inode)
		binary.LittleEndian.PutUint16(block[offset+4:], uint16(recLen))
		block[offset+6] = uint8(nameLen)
		block[offset+7] = entry.Type
		copy(block[offset+8:], entry.Name)

		offset += recLen
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
	// its dx_root index (the corruption this whole feature fixes). On the first
	// such insert, flatten the directory to linear and queue it for re-index at
	// finalize; then fall through to the ordinary linear insert below.
	if inode.Flags&inodeFlagIndex != 0 {
		if err := b.prepareHtreeForMutation(dirInode); err != nil {
			return err
		}
		inode, err = b.readLiveDirInode(dirInode)
		if err != nil {
			return fmt.Errorf("failed to re-read flattened directory inode: %w", err)
		}
	}

	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return fmt.Errorf("failed to get directory blocks: %w", err)
	}

	newRecLen := dirRecLen(len(entry.Name))

	for _, blockNum := range dataBlocks {
		if success, err := b.tryAddEntryToBlock(blockNum, entry, newRecLen); err != nil {
			return fmt.Errorf("failed to add entry to directory block %d: %w", blockNum, err)
		} else if success {
			return nil
		}
	}

	// Allocate new block
	newBlock, err := b.allocateBlock()
	if err != nil {
		return err
	}

	if err := b.addBlockToInode(dirInode, newBlock); err != nil {
		return err
	}

	block := make([]byte, blockSize)
	binary.LittleEndian.PutUint32(block[0:], entry.Inode)
	binary.LittleEndian.PutUint16(block[4:], uint16(blockSize))
	block[6] = uint8(len(entry.Name))
	block[7] = entry.Type
	copy(block[8:], entry.Name)

	if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(newBlock))); err != nil {
		return fmt.Errorf("failed to write directory block: %w", err)
	}

	inode, err = b.readInode(dirInode)
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
func (b *builder) tryAddEntryToBlock(blockNum uint32, entry dirEntry, newRecLen int) (bool, error) {
	block := make([]byte, blockSize)
	if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
		return false, fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
	}

	offset := 0
	lastOffset := 0

	for offset < blockSize {
		recLen := binary.LittleEndian.Uint16(block[offset+4:])
		if recLen == 0 {
			break
		}

		lastOffset = offset
		offset += int(recLen)
	}

	lastActualSize := dirRecLen(int(block[lastOffset+6]))

	lastRecLen := int(binary.LittleEndian.Uint16(block[lastOffset+4:]))

	spaceAvailable := lastRecLen - lastActualSize
	if spaceAvailable < newRecLen {
		return false, nil
	}

	binary.LittleEndian.PutUint16(block[lastOffset+4:], uint16(lastActualSize))

	newOffset := lastOffset + lastActualSize
	remaining := blockSize - newOffset

	binary.LittleEndian.PutUint32(block[newOffset:], entry.Inode)
	binary.LittleEndian.PutUint16(block[newOffset+4:], uint16(remaining))
	block[newOffset+6] = uint8(len(entry.Name))
	block[newOffset+7] = entry.Type
	copy(block[newOffset+8:], entry.Name)

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

	dataBlocks, err := b.getInodeBlocks(inode)
	if err != nil {
		return fmt.Errorf("failed to get directory blocks for entry removal: %w", err)
	}

	for _, blockNum := range dataBlocks {
		block := make([]byte, blockSize)
		if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
			return fmt.Errorf("failed to read directory block %d: %w", blockNum, err)
		}

		offset := 0
		prevOffset := -1

		for offset < blockSize {
			recLen := binary.LittleEndian.Uint16(block[offset+4:])
			if recLen == 0 {
				break
			}

			nameLen := int(block[offset+6])
			entryName := string(block[offset+8 : offset+8+nameLen])

			if entryName == name {
				if prevOffset < 0 {
					// First entry in block: set inode to 0 to mark as unused
					binary.LittleEndian.PutUint32(block[offset:], 0)
				} else {
					// Not first entry: expand previous entry's rec_len
					prevRecLen := binary.LittleEndian.Uint16(block[prevOffset+4:])
					binary.LittleEndian.PutUint16(block[prevOffset+4:], prevRecLen+recLen)
				}

				if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(blockNum))); err != nil {
					return fmt.Errorf("failed to write directory block %d: %w", blockNum, err)
				}

				return nil
			}

			prevOffset = offset
			offset += int(recLen)
		}
	}

	return fmt.Errorf("entry %q not found in directory", name)
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

// parseDirentBlock returns the real entries in a directory block and the parent
// inode if the block carries a ".." record (0 otherwise). Because it feeds the
// destructive flatten/rebuild of foreign directories, it validates rec_len and
// name_len against the block bounds, erroring on a malformed on-disk dirent rather
// than slicing out of bounds and panicking.
func parseDirentBlock(block []byte, dirInode uint32) ([]dirEntry, uint32, error) {
	var (
		entries     []dirEntry
		parentInode uint32
	)
	for offset := 0; offset < blockSize; {
		if offset+8 > blockSize {
			return nil, 0, fmt.Errorf("directory inode %d: truncated dirent at offset %d", dirInode, offset)
		}
		recLen := int(binary.LittleEndian.Uint16(block[offset+4:]))
		if recLen == 0 {
			break
		}
		if recLen < 8 || recLen%4 != 0 || offset+recLen > blockSize {
			return nil, 0, fmt.Errorf("directory inode %d: invalid rec_len %d at offset %d", dirInode, recLen, offset)
		}

		if entryInode := binary.LittleEndian.Uint32(block[offset:]); entryInode != 0 {
			nameLen := int(block[offset+6])
			if nameLen > recLen-8 {
				return nil, 0, fmt.Errorf("directory inode %d: invalid name_len %d at offset %d", dirInode, nameLen, offset)
			}
			name := string(block[offset+8 : offset+8+nameLen])

			switch name {
			case ".":
				// The directory's own inode; callers re-synthesize it.
			case "..":
				parentInode = entryInode
			default:
				entries = append(entries, dirEntry{Inode: entryInode, Type: block[offset+7], Name: []byte(name)})
			}
		}

		offset += recLen
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

		offset := 0
		for offset < blockSize {
			recLen := binary.LittleEndian.Uint16(block[offset+4:])
			if recLen == 0 {
				break
			}

			nameLen := int(block[offset+6])
			entryName := string(block[offset+8 : offset+8+nameLen])

			if entryName == name {
				return binary.LittleEndian.Uint32(block[offset:]), nil
			}

			offset += int(recLen)
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

		offset := 0
		for offset < blockSize {
			recLen := binary.LittleEndian.Uint16(block[offset+4:])
			if recLen == 0 {
				break
			}

			entryInode := binary.LittleEndian.Uint32(block[offset:])
			if entryInode != 0 {
				nameLen := int(block[offset+6])
				entryName := string(block[offset+8 : offset+8+nameLen])
				entryType := block[offset+7]

				// Skip "." and ".."
				if entryName != "." && entryName != ".." {
					entries = append(entries, dirEntry{
						Inode: entryInode,
						Type:  entryType,
						Name:  []byte(entryName),
					})
				}
			}

			offset += int(recLen)
		}
	}

	return entries, nil
}
