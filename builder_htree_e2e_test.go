package ext4fs_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pilat/go-ext4fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// highByteNames are filenames containing bytes >= 0x80, used so open()-by-name
// exercises the signed/unsigned hash distinction on disk.
var highByteNames = []string{"café", "naïve", "Москва", "日本語"}

// dxRootFields parses the dx_root header fields from logical block 0.
func dxRootFields(block []byte) (limit, count uint16, indirectLevels, hashVersion uint8) {
	hashVersion = block[0x18+4]
	indirectLevels = block[0x18+6]
	limit = binary.LittleEndian.Uint16(block[0x20:])
	count = binary.LittleEndian.Uint16(block[0x22:])
	return limit, count, indirectLevels, hashVersion
}

// assertHtreeInode checks the inode-level invariants of a freshly emitted htree
// directory: INDEX flag set, single contiguous extent, i_size and i_blocks
// consistent with the block count, and a well-formed depth-1 dx_root with the
// exact no-csum limit.
func assertHtreeInode(t *testing.T, img *ext4fs.Image, dirInode uint32) (blockCount int) {
	t.Helper()

	flags, sizeLo, blocksLo, extEntries, extDepth, err := img.InodeFieldsForTest(dirInode)
	require.NoError(t, err)
	assert.NotZero(t, flags&ext4fs.InodeFlagIndexForTest, "EXT4_INDEX_FL must be set")

	blocks, err := img.DirBlocksForTest(dirInode)
	require.NoError(t, err)
	k1 := len(blocks)
	require.GreaterOrEqual(t, k1, 2, "htree needs at least root + 1 leaf")

	assert.Equal(t, uint32(k1)*4096, sizeLo, "i_size must be (K+1)*blocksize")
	assert.Equal(t, uint32(k1)*8, blocksLo, "i_blocks must be (K+1)*8 sectors (single extent, no xattr)")
	assert.Equal(t, uint16(0), extDepth, "contiguous emit must be a depth-0 extent tree")
	assert.Equal(t, uint16(1), extEntries, "contiguous emit must be a single extent")

	// Logical blocks must be a contiguous run 0..K with no gap.
	for i := 1; i < len(blocks); i++ {
		assert.Equal(t, blocks[0]+uint32(i), blocks[i], "directory blocks must be physically contiguous here")
	}

	root, err := img.ReadBlockForTest(blocks[0])
	require.NoError(t, err)
	limit, count, indirect, hashVer := dxRootFields(root)
	assert.Equal(t, uint16(ext4fs.DxRootLimitForTest), limit, "dx_root limit must be exactly 508 (no csum)")
	assert.Equal(t, uint16(k1-1), count, "dx_root count must equal the number of leaves")
	assert.Equal(t, uint8(0), indirect, "must be depth-1 (indirect_levels 0)")
	assert.Equal(t, uint8(ext4fs.HashVersionHalfMD4ForTest), hashVer, "dx_root hash version must be half_md4")

	return k1
}

// createDirWithFiles makes directory "d" under root and populates it with count
// f%05d files plus the high-byte names, returning the dir inode and full name set.
func createDirWithFiles(t *testing.T, env *testEnv, count int) (dirInode uint32, names []string) {
	t.Helper()
	dirInode, err := env.builder.CreateDirectory(ext4fs.RootInode, "d", 0755, 0, 0)
	require.NoError(t, err)
	for i := 0; i < count; i++ {
		n := fmt.Sprintf("file_%05d.txt", i)
		_, err := env.builder.CreateFile(dirInode, n, []byte("x"), 0644, 0, 0)
		require.NoError(t, err)
		names = append(names, n)
	}
	for _, n := range highByteNames {
		_, err := env.builder.CreateFile(dirInode, n, []byte("y"), 0644, 0, 0)
		require.NoError(t, err)
		names = append(names, n)
	}
	return dirInode, names
}

// TestEmitOwnHtree is the Task-4 acceptance gate for our own images: emit a
// many-entry directory as a depth-1 htree, then prove the on-disk inode invariants
// and that the kernel mounts it, lists every entry, and opens every file by name —
// including names with bytes >= 0x80, which fail under a wrong signedness.
func TestEmitOwnHtree(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 64)
	dirInode, names := createDirWithFiles(t, env, 700)

	require.NoError(t, env.builder.EmitHtreeForTest(dirInode))
	assertHtreeInode(t, env.builder, dirInode)

	env.finalize()

	cmds := []string{
		"cd d",
		"ls -1 | wc -l",
	}
	for _, n := range []string{"file_00000.txt", "file_00350.txt", "file_00699.txt", "café", "naïve", "Москва", "日本語"} {
		cmds = append(cmds, fmt.Sprintf(`test -f "%s" && echo "OK %s"`, n, n))
	}
	out := env.dockerExecSimple(cmds...)

	assert.Contains(t, out, fmt.Sprintf("%d", len(names)))
	for _, n := range []string{"file_00000.txt", "file_00350.txt", "file_00699.txt", "café", "naïve", "Москва", "日本語"} {
		assert.Contains(t, out, "OK "+n, "open-by-name must succeed for %q", n)
	}

	// dumpe2fs must report dir_index now that an own directory is indexed.
	assert.Contains(t, env.dumpe2fsHeader(), "dir_index", "dir_index feature must be set")
	assert.Contains(t, env.dumpe2fsHeader(), "signed_directory_hash", "own images must record signed hash")

	// Exhaustive hash oracle: every one of the 700 + high-byte entries opens by name.
	assertAllOpenByName(t, env, "d", names)
}

// TestEmitHtreeWithDirXattr covers the xattr-preservation branch of the rebuild:
// a directory that carries an extended attribute (its own xattr block) is indexed.
// The xattr block must survive the free-all+realloc reconcile, i_blocks must
// account for it (data blocks + 1 xattr block), and the attribute must still be
// readable after mount.
func TestEmitHtreeWithDirXattr(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 64)
	dir, err := env.builder.CreateDirectory(ext4fs.RootInode, "d", 0755, 0, 0)
	require.NoError(t, err)
	require.NoError(t, env.builder.SetXattr(dir, "user.htree_test", []byte("dir-xattr-value")))

	for i := 0; i < 700; i++ {
		_, err := env.builder.CreateFile(dir, fmt.Sprintf("file_%05d.txt", i), []byte("x"), 0644, 0, 0)
		require.NoError(t, err)
	}

	require.NoError(t, env.builder.EmitHtreeForTest(dir))

	flags, _, blocksLo, _, _, err := env.builder.InodeFieldsForTest(dir)
	require.NoError(t, err)
	require.NotZero(t, flags&ext4fs.InodeFlagIndexForTest)
	blocks, err := env.builder.DirBlocksForTest(dir)
	require.NoError(t, err)
	// i_blocks = (K+1) data blocks + 1 preserved xattr block, in 512-byte sectors.
	assert.Equal(t, uint32(len(blocks)+1)*8, blocksLo, "i_blocks must include the preserved xattr block")

	env.finalize()

	out := env.dockerExecSimple(
		`getfattr -n user.htree_test --only-values d`,
		`test -f d/file_00000.txt && echo "OK first"`,
		`test -f d/file_00699.txt && echo "OK last"`,
	)
	assert.Contains(t, out, "dir-xattr-value", "directory xattr must survive the htree rebuild")
	assert.Contains(t, out, "OK first")
	assert.Contains(t, out, "OK last")
}

// TestEmitHtreeWithHardlinks exercises the plan's "preserve hardlinks verbatim
// through rebuild" constraint: a directory holding many hard links to one inode
// (alongside regular files) is indexed at finalize. The rebuild enumerates every
// name independently and hashes it into its own leaf, so all link names must open
// by name, the target's link count must equal the number of names pointing at it,
// and a link must resolve to the same inode as the target.
func TestEmitHtreeWithHardlinks(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 64)
	dir, err := env.builder.CreateDirectory(ext4fs.RootInode, "d", 0755, 0, 0)
	require.NoError(t, err)

	var names []string
	for i := 0; i < 300; i++ {
		n := fmt.Sprintf("file_%04d", i)
		_, err := env.builder.CreateFile(dir, n, []byte("regular"), 0644, 0, 0)
		require.NoError(t, err)
		names = append(names, n)
	}
	target, err := env.builder.CreateFile(dir, "target.bin", []byte("shared"), 0644, 0, 0)
	require.NoError(t, err)
	names = append(names, "target.bin")
	const links = 300
	for i := 0; i < links; i++ {
		n := fmt.Sprintf("link_%04d", i)
		require.NoError(t, env.builder.Link(dir, n, target))
		names = append(names, n)
	}

	env.finalize() // auto-index drives the rebuild over the hardlink set

	flags := env.inodeFlagsByPath(t, "/d")
	require.NotZero(t, flags&ext4fs.InodeFlagIndexForTest, "directory must be htree-indexed")

	// Every name (regular + target + every link) opens by name after the rebuild.
	assertAllOpenByName(t, env, "d", names)

	// The shared inode keeps the right link count, and a link resolves to it.
	out := env.dockerExecSimple(
		"cd d",
		`echo "LINKS:$(stat -c %h target.bin)"`,
		`echo "SAME:$([ "$(stat -c %i target.bin)" = "$(stat -c %i link_0000)" ] && echo yes)"`,
	)
	assert.Contains(t, out, fmt.Sprintf("LINKS:%d", links+1), "target link count must be 1 + number of hard links")
	assert.Contains(t, out, "SAME:yes", "a hard link must resolve to the target's inode")
}

// TestOwnHtreeAutoIndexLongNames drives the automatic finalize path (Save emits
// htree directories) with 1000 long filenames and adds the open-by-name hash
// oracle the bare `ls | wc -l` checks lack: a wrong hash would still list the
// files but fail to open them by name.
func TestOwnHtreeAutoIndexLongNames(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 128)
	dir, err := env.builder.CreateDirectory(ext4fs.RootInode, "bigdir", 0755, 0, 0)
	require.NoError(t, err)

	const n = 1000
	var names []string
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("very_long_file_name_that_takes_up_space_%04d.txt", i)
		_, err = env.builder.CreateFile(dir, name, []byte("content"), 0644, 0, 0)
		require.NoError(t, err)
		names = append(names, name)
	}

	env.finalize() // Save auto-indexes the directory

	flags := env.inodeFlagsByPath(t, "/bigdir")
	assert.NotZero(t, flags&ext4fs.InodeFlagIndexForTest, "directory should be htree-indexed, flags=0x%x", flags)

	out := env.dockerExecSimple("ls -1 bigdir | wc -l")
	assert.Equal(t, fmt.Sprintf("%d", n), lastLine(out))

	// Exhaustive hash oracle over the whole production auto-index path.
	assertAllOpenByName(t, env, "bigdir", names)
}

// TestOwnHtreeDeletedEntriesStayLinear validates the live-entry trigger: a
// directory that grew large and then had every entry deleted has ~0 live entries,
// so it fits one block and must stay linear (no EXT4_INDEX_FL), even though it
// still holds the blocks it grew into (the library never shrinks on delete).
func TestOwnHtreeDeletedEntriesStayLinear(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 128)
	dir, err := env.builder.CreateDirectory(ext4fs.RootInode, "shrunk", 0755, 0, 0)
	require.NoError(t, err)

	const n = 600
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("file_%05d.txt", i)
		_, err = env.builder.CreateFile(dir, names[i], []byte("x"), 0644, 0, 0)
		require.NoError(t, err)
	}
	for _, name := range names {
		require.NoError(t, env.builder.Delete(dir, name))
	}

	env.finalize()

	flags := env.inodeFlagsByPath(t, "/shrunk")
	assert.Zero(t, flags&ext4fs.InodeFlagIndexForTest, "emptied directory must stay linear, flags=0x%x", flags)

	out := env.dockerExecSimple("ls -1 shrunk | wc -l")
	assert.Equal(t, "0", lastLine(out), "directory must be empty")
}

// TestOwnHtreeLinearFallback forces the depth-1 overflow: a directory whose live
// entries need more than the 508 dx_root leaves must fall back to a linear
// directory — valid, no error, e2fsck-clean — rather than failing.
//
// The entries are hard links to a single file. That is the realistic way to grow
// a directory past ~508 blocks: ordinary files interleave a data block per file,
// fragmenting the directory past the incremental-growth extent cap before it ever
// gets that large, whereas links allocate no data blocks, so the directory's
// blocks stay contiguous (a single extent). It also exercises hardlink
// preservation through the (declined) rebuild — each name hashes independently.
func TestOwnHtreeLinearFallback(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 256)
	dir, err := env.builder.CreateDirectory(ext4fs.RootInode, "huge", 0755, 0, 0)
	require.NoError(t, err)

	suffix := strings.Repeat("x", 250) // 254-char names pack 15 per 4 KiB leaf
	target, err := env.builder.CreateFile(dir, "0000"+suffix, []byte("payload"), 0644, 0, 0)
	require.NoError(t, err)

	// >508 leaves needs >7620 entries; create that many distinct-named hard links.
	const n = 7800
	for i := 1; i < n; i++ {
		name := fmt.Sprintf("%04d%s", i, suffix)
		require.NoError(t, env.builder.Link(dir, name, target))
	}

	env.finalize() // must not error despite exceeding the depth-1 bound

	flags := env.inodeFlagsByPath(t, "/huge")
	assert.Zero(t, flags&ext4fs.InodeFlagIndexForTest, "oversized directory must stay linear, flags=0x%x", flags)

	out := env.dockerExecSimple("ls -1 huge | wc -l")
	assert.Equal(t, fmt.Sprintf("%d", n), lastLine(out), "all entries must survive the linear fallback")
}

// TestOwnHtreeMultiGroupBackupSB checks decision 8's backup requirement: when a
// directory is indexed, the dir_index feature and signedness flag must be written
// to the primary AND every backup superblock, or e2fsck reports a backup feature
// mismatch. It uses a two-group image (>128 MiB) so a sparse backup superblock
// exists in group 1, and asserts the flags landed there directly.
func TestOwnHtreeMultiGroupBackupSB(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 192) // 192 MiB -> 2 block groups; group 1 is a sparse backup
	dir, err := env.builder.CreateDirectory(ext4fs.RootInode, "d", 0755, 0, 0)
	require.NoError(t, err)
	for i := 0; i < 700; i++ {
		_, err := env.builder.CreateFile(dir, fmt.Sprintf("file_%05d.txt", i), []byte("x"), 0644, 0, 0)
		require.NoError(t, err)
	}

	env.finalize() // mount script runs e2fsck -n -f, which checks backup consistency

	out := env.dockerExecSimple("ls -1 d | wc -l", `test -f "d/file_00000.txt" && echo OK`)
	assert.Contains(t, out, "700")
	assert.Contains(t, out, "OK")
	assert.Contains(t, env.dumpe2fsHeader(), "dir_index")

	// Read group 1's backup superblock directly (block 32768, no 1024 offset).
	f, err := os.Open(env.imagePath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	backup := make([]byte, 1024)
	_, err = f.ReadAt(backup, int64(32768)*4096)
	require.NoError(t, err)

	feat := binary.LittleEndian.Uint32(backup[0x5C:])
	flags := binary.LittleEndian.Uint32(backup[0x160:])
	assert.NotZero(t, feat&ext4fs.CompatDirIndexForTest, "backup SB must carry dir_index")
	assert.NotZero(t, flags&ext4fs.FlagsSignedHashForTest, "backup SB must carry the signed-hash flag")
}

// TestFlattenEmitRoundTrip verifies flattenHtree reverses an emit (clearing the
// index and recovering every entry as linear) and that a subsequent emit rebuilds
// a valid htree over the identical entry set.
func TestFlattenEmitRoundTrip(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 64)
	dirInode, names := createDirWithFiles(t, env, 600)

	require.NoError(t, env.builder.EmitHtreeForTest(dirInode))

	// Flatten back to linear.
	require.NoError(t, env.builder.FlattenHtreeForTest(dirInode))
	flags, _, _, _, _, err := env.builder.InodeFieldsForTest(dirInode)
	require.NoError(t, err)
	assert.Zero(t, flags&ext4fs.InodeFlagIndexForTest, "flatten must clear EXT4_INDEX_FL")

	listed, err := env.builder.ListNamesForTest(dirInode)
	require.NoError(t, err)
	sameNameSet(t, listed, names, "after flatten")

	// Re-emit and validate end to end.
	require.NoError(t, env.builder.EmitHtreeForTest(dirInode))
	assertHtreeInode(t, env.builder, dirInode)

	env.finalize()

	out := env.dockerExecSimple("ls -1 d | wc -l")
	assert.Equal(t, fmt.Sprintf("%d", len(names)), lastLine(out))
	// Exhaustive: every entry survives the flatten -> re-emit round-trip.
	assertAllOpenByName(t, env, "d", names)
}

// TestEmitSurplusReconcile reproduces the surplus-block hazard in isolation: a
// directory that holds far more blocks than its surviving entries need. After a
// large emit and bulk deletes (which never shrink the directory), a re-emit must
// repack into exactly K+1 contiguous blocks, return the surplus to free, keep
// i_size/i_blocks consistent, and stay e2fsck-clean.
func TestEmitSurplusReconcile(t *testing.T) {
	skipIfNoDocker(t)

	env := newTestEnv(t, 64)
	dirInode, names := createDirWithFiles(t, env, 1200)

	require.NoError(t, env.builder.EmitHtreeForTest(dirInode))
	bigBlocks, err := env.builder.DirBlocksForTest(dirInode)
	require.NoError(t, err)

	// Delete all but the high-byte names plus a couple of ASCII survivors.
	survivors := map[string]bool{"café": true, "naïve": true, "Москва": true, "日本語": true, "file_00000.txt": true, "file_01199.txt": true}
	for _, n := range names {
		if !survivors[n] {
			require.NoError(t, env.builder.Delete(dirInode, n))
		}
	}

	// Delete does not shrink: the directory still holds all its blocks.
	stillBlocks, err := env.builder.DirBlocksForTest(dirInode)
	require.NoError(t, err)
	assert.Equal(t, len(bigBlocks), len(stillBlocks), "delete must not shrink the directory")

	// Re-emit: the survivors pack into far fewer leaves; surplus must be freed.
	require.NoError(t, env.builder.EmitHtreeForTest(dirInode))
	smallK1 := assertHtreeInode(t, env.builder, dirInode)
	assert.Less(t, smallK1, len(bigBlocks), "re-emit must repack into fewer blocks than before")

	env.finalize() // e2fsck (run by dockerExec) proves the surplus was freed cleanly

	survivorNames := make([]string, 0, len(survivors))
	for n := range survivors {
		survivorNames = append(survivorNames, n)
	}
	out := env.dockerExecSimple("ls -1 d | wc -l")
	assert.Equal(t, fmt.Sprintf("%d", len(survivors)), lastLine(out))
	// Exhaustive: every survivor opens by name after the repack.
	assertAllOpenByName(t, env, "d", survivorNames)
}
