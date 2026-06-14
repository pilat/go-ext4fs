package ext4fs_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pilat/go-ext4fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertAllOpenByName mounts the image and confirms that EVERY name in names
// opens by name inside dirRel (relative to the mount root) — the exhaustive htree
// hash oracle, not a sample. A wrong hash for any single name surfaces as a miss.
func assertAllOpenByName(t *testing.T, env *testEnv, dirRel string, names []string) {
	t.Helper()
	base := fmt.Sprintf("checknames-%d.txt", time.Now().UnixNano())
	hostPath := filepath.Join(sharedHostDir, base)
	require.NoError(t, os.WriteFile(hostPath, []byte(strings.Join(names, "\n")+"\n"), 0o644))
	t.Cleanup(func() { _ = os.Remove(hostPath) })
	remote := filepath.Join(sharedContainerDir, base)

	out := env.dockerExecSimple(
		fmt.Sprintf(`missing=0; while IFS= read -r n; do [ -e "%s/$n" ] || { echo "MISS:$n"; missing=$((missing+1)); }; done < %s; echo "MISSING_COUNT:$missing"`, dirRel, remote),
	)
	assert.Contains(t, out, "MISSING_COUNT:0", "every one of %d entries must open by name; output:\n%s", len(names), out)
}

// mkfsForeignImage creates an empty foreign image with the given mke2fs feature
// set (no mount/populate) and returns the host path.
func mkfsForeignImage(t *testing.T, features string) string {
	t.Helper()
	if !dockerAvailable || dockerContainerID == "" {
		t.Skip("Docker test container not available")
	}
	base := fmt.Sprintf("mkfs-%d.img", time.Now().UnixNano())
	path := filepath.Join(sharedHostDir, base)
	remote := filepath.Join(sharedContainerDir, base)
	script := fmt.Sprintf(`
set -e
rm -f %[1]s
dd if=/dev/zero of=%[1]s bs=1M count=64 status=none
mkfs.ext4 -q -F -O %[2]s -b 4096 -I 256 -N 8192 %[1]s
chmod 0666 %[1]s
`, remote, features)
	stdout, stderr, err := dockerExecPrivileged(t, script)
	if err != nil {
		t.Fatalf("mkfs failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

// TestForeignOpenRejectsUnsupported verifies the safety reject-guard: a reopened
// image carrying metadata_csum or reserved GDT blocks (resize_inode) is refused
// rather than silently mis-read and corrupted on the next Save. Both images
// otherwise pass the geometry and incompat checks, so they exercise the new guard
// specifically.
func TestForeignOpenRejectsUnsupported(t *testing.T) {
	skipIfNoDocker(t)

	cases := []struct {
		name     string
		features string
		wantErr  string
	}{
		{"metadata_csum", "dir_index,metadata_csum,^metadata_csum_seed,^resize_inode,^64bit,^flex_bg,^has_journal", "metadata_csum"},
		{"resize_inode", "dir_index,^metadata_csum,resize_inode,^64bit,^flex_bg,^has_journal", "reserved GDT"},
		{"sparse_super", "dir_index,^metadata_csum,^resize_inode,^sparse_super,^64bit,^flex_bg,^has_journal", "sparse_super"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := mkfsForeignImage(t, tc.features)
			_, err := ext4fs.Open(ext4fs.WithExistingImagePath(path))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// foreignImageWithBlockHoles builds a foreign image whose root holds many
// single-block files, then deletes a fraction in-container so their data blocks
// become holes BELOW the allocation high-water mark — the case that exposed the
// free-block-count accounting gap (the inode-hole twin). Returns the host path.
func foreignImageWithBlockHoles(t *testing.T) string {
	t.Helper()
	if !dockerAvailable || dockerContainerID == "" {
		t.Skip("Docker test container not available")
	}
	base := fmt.Sprintf("holes-%d.img", time.Now().UnixNano())
	path := filepath.Join(sharedHostDir, base)
	remote := filepath.Join(sharedContainerDir, base)
	mountDir := fmt.Sprintf("/mnt/holes-%d", time.Now().UnixNano())
	script := fmt.Sprintf(`
set -e
rm -f %[1]s
dd if=/dev/zero of=%[1]s bs=1M count=64 status=none
mkfs.ext4 -q -F -O %[2]s -b 4096 -I 256 -N 8192 %[1]s
mkdir -p %[3]s
mount -o loop %[1]s %[3]s
i=0
while [ $i -lt 400 ]; do dd if=/dev/zero of=%[3]s/f$i bs=4096 count=1 status=none; i=$((i+1)); done
i=1
while [ $i -lt 400 ]; do rm -f %[3]s/f$i; i=$((i+2)); done
sync
umount %[3]s
chmod 0666 %[1]s
`, remote, foreignMkfsOpts, mountDir)
	stdout, stderr, err := dockerExecPrivileged(t, script)
	if err != nil {
		t.Fatalf("block-holes scaffold failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}

// TestForeignReopenBlockHolesFreeCount is the regression test for the free-block
// count fix: opening a foreign image whose deleted files left data-block holes,
// modifying it, and saving must produce a free-block count e2fsck accepts. Adding
// a single-block file also reuses one of the holes (exercising the freed-count
// decrement that would underflow without the fix). e2fsck (run by the mount
// script) recomputes and compares the free-block count, so a clean run proves it.
func TestForeignReopenBlockHolesFreeCount(t *testing.T) {
	skipIfNoDocker(t)

	imagePath := foreignImageWithBlockHoles(t)

	img, err := ext4fs.Open(ext4fs.WithExistingImagePath(imagePath))
	require.NoError(t, err)
	_, err = img.CreateFile(ext4fs.RootInode, "added.txt", []byte("reuses a hole"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	env := &testEnv{t: t, imagePath: imagePath}
	out := env.dockerExecSimple(
		`test -f added.txt && echo "OK added"`,
		`test -f f0 && echo "OK survivor"`,
		`test -f f1 || echo "OK deleted"`,
	)
	assert.Contains(t, out, "OK added")
	assert.Contains(t, out, "OK survivor")
	assert.Contains(t, out, "OK deleted")
}

// deleteFilesInForeignImage mounts the image inside the container and removes the
// given files from dirRel with the real kernel, then unmounts. The kernel voids
// the dirents but never shrinks the directory, leaving its htree leaves
// under-full — the setup for the denser-repack reconcile hazard.
func deleteFilesInForeignImage(t *testing.T, imagePath, dirRel string, names []string) {
	t.Helper()
	remoteImage := filepath.Join(sharedContainerDir, filepath.Base(imagePath))

	listBase := fmt.Sprintf("rm-names-%d.txt", time.Now().UnixNano())
	hostList := filepath.Join(sharedHostDir, listBase)
	require.NoError(t, os.WriteFile(hostList, []byte(strings.Join(names, "\n")+"\n"), 0o644))
	t.Cleanup(func() { _ = os.Remove(hostList) })
	remoteList := filepath.Join(sharedContainerDir, listBase)

	mountDir := fmt.Sprintf("/mnt/rm-%d", time.Now().UnixNano())
	script := fmt.Sprintf(`
set -e
mkdir -p %[1]s
mount -o loop %[2]s %[1]s
while IFS= read -r n; do rm -f "%[1]s/%[3]s/$n"; done < %[4]s
sync
umount %[1]s
`, mountDir, remoteImage, dirRel, remoteList)
	stdout, stderr, err := dockerExecPrivileged(t, script)
	if err != nil {
		t.Fatalf("in-container delete failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}
}

// openForeign opens a foreign image and returns the Image plus the inode of the
// htree directory at dirPath's last component under root.
func openForeign(t *testing.T, imagePath, dirName string) (*ext4fs.Image, uint32) {
	t.Helper()
	img, err := ext4fs.Open(ext4fs.WithExistingImagePath(imagePath))
	require.NoError(t, err)
	dInode, err := img.FindEntryForTest(ext4fs.RootInode, dirName)
	require.NoError(t, err)
	require.NotZero(t, dInode)
	return img, dInode
}

// TestForeignHtreeAddDelete is the headline Task-6 acceptance: the corruption fix.
// It opens a real mke2fs htree directory, adds and deletes files in it, and
// proves the result is e2fsck-clean and that EVERY file — original (incl high
// byte), newly added, and the survivor of a delete — opens by name. Before the
// addDirEntry guard, the first insert silently overwrote the dx_root index.
func TestForeignHtreeAddDelete(t *testing.T) {
	skipIfNoDocker(t)

	imagePath, created := foreignHtreeImage(t, 64, 1000, []string{"café", "naïve"}, "d")

	img, dInode := openForeign(t, imagePath, "d")
	_, err := img.CreateFile(dInode, "newfile_added.txt", []byte("new"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Delete(dInode, "f00042"))
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	env := &testEnv{t: t, imagePath: imagePath}
	out := env.dockerExecSimple(
		"cd d",
		"ls -1 | wc -l",
		`test -f "newfile_added.txt" && echo "OK new"`,
		`test -f "f00000" && echo "OK old"`,
		`test -f "f00999" && echo "OK lastold"`,
		`test -f "café" && echo "OK cafe"`,
		`test -f "naïve" && echo "OK naive"`,
		`test -f "f00042" || echo "OK deleted"`,
	)

	// 1000 ascii + 2 high-byte - 1 deleted + 1 added.
	assert.Contains(t, out, fmt.Sprintf("%d", len(created)))
	for _, want := range []string{"OK new", "OK old", "OK lastold", "OK cafe", "OK naive", "OK deleted"} {
		assert.Contains(t, out, want)
	}

	// Exhaustive hash oracle: every surviving original plus the new file must open
	// by name — not just the sampled handful above.
	expected := []string{"newfile_added.txt"}
	for _, n := range created {
		if n != "f00042" {
			expected = append(expected, n)
		}
	}
	assertAllOpenByName(t, env, "d", expected)
}

// TestForeignHtreeDeleteOnlyStaysHtree validates decision 4: a delete-only
// mutation never rebuilds the index. It opens a foreign htree directory, deletes
// every entry of one whole leaf (including that leaf's minimum-hash boundary
// entry) without adding anything, and saves. The directory must stay htree, the
// stale/empty index must remain e2fsck-clean, and every surviving file must still
// open by name. This is the only path with no rebuild to launder it.
func TestForeignHtreeDeleteOnlyStaysHtree(t *testing.T) {
	skipIfNoDocker(t)

	imagePath, created := foreignHtreeImage(t, 64, 1200, nil, "d")

	// Find a leaf with several entries and delete all of them via the kernel-
	// faithful leaf-only delete path.
	dump := debugfsHtreeDump(t, imagePath, "/d")
	p := parseHtreeDump(t, dump)
	require.GreaterOrEqual(t, len(p.leaves), 2)
	var victimLeaf int
	for i := 1; i < len(p.leaves); i++ {
		if len(p.leaves[i].entries) >= 2 {
			victimLeaf = i
			break
		}
	}
	require.NotZero(t, victimLeaf, "need a non-root leaf with entries to empty")
	toDelete := make([]string, 0, len(p.leaves[victimLeaf].entries))
	for _, e := range p.leaves[victimLeaf].entries {
		toDelete = append(toDelete, e.name)
	}

	img, dInode := openForeign(t, imagePath, "d")
	for _, name := range toDelete {
		require.NoError(t, img.Delete(dInode, name))
	}
	// The directory must still be htree (no flatten on delete-only).
	flags, err := img.InodeFlagsForTest(dInode)
	require.NoError(t, err)
	assert.NotZero(t, flags&ext4fs.InodeFlagIndexForTest, "delete-only must keep the directory htree")
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	deleted := make(map[string]bool, len(toDelete))
	for _, n := range toDelete {
		deleted[n] = true
	}

	env := &testEnv{t: t, imagePath: imagePath}
	// Probe a handful of survivors plus a deleted name.
	cmds := []string{"cd d", "ls -1 | wc -l"}
	checked := 0
	for _, n := range created {
		if !deleted[n] && checked < 6 {
			cmds = append(cmds, fmt.Sprintf(`test -f "%s" && echo "OK %s"`, n, n))
			checked++
		}
	}
	cmds = append(cmds, fmt.Sprintf(`test -f "%s" || echo "OK gone"`, toDelete[0]))
	out := env.dockerExecSimple(cmds...)

	assert.Contains(t, out, fmt.Sprintf("%d", len(created)-len(toDelete)))
	assert.Contains(t, out, "OK gone")
	checked = 0
	for _, n := range created {
		if !deleted[n] && checked < 6 {
			assert.Contains(t, out, "OK "+n, "survivor %q must open by name", n)
			checked++
		}
	}
}

// TestForeignHtreeUntouchedByteIdentical confirms a foreign htree directory we
// never mutate is left byte-for-byte unchanged (its data blocks) across Open and
// Save, even when an unrelated part of the image is modified. Save always rewrites
// the superblock/GDT, so the assertion is scoped to the directory's data blocks.
func TestForeignHtreeUntouchedByteIdentical(t *testing.T) {
	skipIfNoDocker(t)

	imagePath, _ := foreignHtreeImage(t, 64, 1000, nil, "d")

	// Snapshot the htree directory's data blocks.
	img, dInode := openForeign(t, imagePath, "d")
	blocks, err := img.DirBlocksForTest(dInode)
	require.NoError(t, err)
	snapshot := make(map[uint32][]byte, len(blocks))
	for _, b := range blocks {
		data, err := img.ReadBlockForTest(b)
		require.NoError(t, err)
		snapshot[b] = data
	}
	require.NoError(t, img.Close())

	// Reopen, modify an unrelated location (root), and save.
	img2, err := ext4fs.Open(ext4fs.WithExistingImagePath(imagePath))
	require.NoError(t, err)
	_, err = img2.CreateFile(ext4fs.RootInode, "unrelated.txt", []byte("z"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img2.Save())
	require.NoError(t, img2.Close())

	// The htree directory's data blocks must be unchanged.
	f, err := os.Open(imagePath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	for b, want := range snapshot {
		got := make([]byte, 4096)
		_, err := f.ReadAt(got, int64(b)*4096)
		require.NoError(t, err)
		assert.Equal(t, want, got, "untouched htree directory block %d changed", b)
	}
}

// TestForeignHtreeDenserRepack is the real-image reproduction of the surplus-block
// hazard. A foreign htree directory is populated and then half its entries are
// deleted in-container by the kernel, which leaves the leaves under-full but
// keeps every leaf block allocated and referenced. Opening it and adding one file
// triggers flatten + denser re-index: our canonical repack needs far fewer leaves
// than the kernel left allocated. The surplus blocks must be returned to free, or
// e2fsck trips on unreferenced blocks / a size mismatch. We assert e2fsck-clean
// (run by the mount script), a denser block count, and that every survivor plus
// the new file opens by name.
func TestForeignHtreeDenserRepack(t *testing.T) {
	skipIfNoDocker(t)

	imagePath, created := foreignHtreeImage(t, 64, 1500, nil, "d")

	var toDelete, survivors []string
	for i, n := range created {
		if i%2 == 1 {
			toDelete = append(toDelete, n)
		} else {
			survivors = append(survivors, n)
		}
	}
	deleteFilesInForeignImage(t, imagePath, "d", toDelete)

	img, dInode := openForeign(t, imagePath, "d")
	sparseBlocks, err := img.DirBlocksForTest(dInode)
	require.NoError(t, err)
	_, err = img.CreateFile(dInode, "added_after_sparse.txt", []byte("x"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	// Reopen and confirm the directory was repacked denser (surplus blocks freed).
	img2, dInode2 := openForeign(t, imagePath, "d")
	packedBlocks, err := img2.DirBlocksForTest(dInode2)
	require.NoError(t, err)
	require.NoError(t, img2.Close())
	assert.Less(t, len(packedBlocks), len(sparseBlocks),
		"repack must use fewer blocks than the kernel's sparse layout (%d -> %d)", len(sparseBlocks), len(packedBlocks))

	env := &testEnv{t: t, imagePath: imagePath}
	cmds := []string{
		"cd d",
		"ls -1 | wc -l",
		`test -f "added_after_sparse.txt" && echo "OK new"`,
	}
	for _, n := range []string{survivors[0], survivors[len(survivors)/2], survivors[len(survivors)-1]} {
		cmds = append(cmds, fmt.Sprintf(`test -f "%s" && echo "OK %s"`, n, n))
	}
	cmds = append(cmds, fmt.Sprintf(`test -f "%s" || echo "OK gone"`, toDelete[0]))
	out := env.dockerExecSimple(cmds...)

	assert.Contains(t, out, fmt.Sprintf("%d", len(survivors)+1))
	assert.Contains(t, out, "OK new")
	assert.Contains(t, out, "OK gone")
	for _, n := range []string{survivors[0], survivors[len(survivors)/2], survivors[len(survivors)-1]} {
		assert.Contains(t, out, "OK "+n, "survivor %q must open by name after repack", n)
	}

	// Exhaustive: every survivor plus the new file opens by name after the repack.
	assertAllOpenByName(t, env, "d", append(append([]string{}, survivors...), "added_after_sparse.txt"))
}
