package ext4fs_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pilat/go-ext4fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForeignScatteredInodeAccounting is the real-kernel companion to the
// deterministic TestInodeAccountingScatteredMultiGroup. mke2fs builds a two-group
// image and the kernel's Orlov allocator scatters the top-level directory inodes
// across both groups (leaving group 0 with free inodes above its local
// high-water). The library reopens that image, adds a file, and saves — which
// rewrites every group's free-inode/itable_unused field and the superblock
// free-inode count. e2fsck -fn must then find no accounting errors; the
// pre-fix global-cursor logic reported "Free inodes count wrong for group #0".
func TestForeignScatteredInodeAccounting(t *testing.T) {
	skipIfNoDocker(t)

	base := fmt.Sprintf("scatter-%d.img", time.Now().UnixNano())
	remote := filepath.Join(sharedContainerDir, base)
	host := filepath.Join(sharedHostDir, base)
	mountDir := fmt.Sprintf("/mnt/scatter-%d", time.Now().UnixNano())

	// 256 MiB => two 8192-inode groups (-N 16384). Create many top-level
	// directories so Orlov spreads their inodes into group 1.
	build := fmt.Sprintf(`
set -e
rm -f %[1]s
dd if=/dev/zero of=%[1]s bs=1M count=256 status=none
mkfs.ext4 -q -F -O dir_index,^metadata_csum,^resize_inode,^64bit,^flex_bg,^has_journal -b 4096 -I 256 -N 16384 %[1]s
mkdir -p %[2]s
mount -o loop %[1]s %[2]s
i=0
while [ $i -lt 200 ]; do mkdir "%[2]s/dir$i"; : > "%[2]s/dir$i/f"; i=$((i+1)); done
sync
umount %[2]s
chmod 0666 %[1]s
`, remote, mountDir)

	if stdout, stderr, err := dockerExecPrivileged(t, build); err != nil {
		t.Fatalf("mkfs/scatter scaffold failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	img, err := ext4fs.Open(ext4fs.WithExistingImagePath(host))
	require.NoError(t, err)
	_, err = img.CreateFile(ext4fs.RootInode, "added-by-lib", []byte("hello\n"), 0o644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	out, _, err := dockerExecPrivileged(t, fmt.Sprintf(`e2fsck -fn %s; echo "FSCK_EXIT:$?"`, remote))
	require.NoError(t, err)
	assert.Contains(t, out, "FSCK_EXIT:0", "e2fsck must report a clean filesystem; output:\n%s", out)
	assert.NotContains(t, out, "Free inodes count wrong", "scattered free-inode accounting must be correct; output:\n%s", out)
	assert.NotContains(t, out, "Inode bitmap differences", "inode bitmap must match the descriptors; output:\n%s", out)
}
