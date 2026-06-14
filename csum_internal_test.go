package ext4fs

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Pure CRC32C primitive tests (Task 1)
// =============================================================================

// TestCRC32CChainingIdentity verifies the running-seed identity
// crc32c(s, a‖b) == crc32c(crc32c(s, a), b). A helper that inverts on only one
// side (the classic hash/crc32 mistake) fails this.
func TestCRC32CChainingIdentity(t *testing.T) {
	full := []byte("the quick brown fox jumps over the lazy dog")
	const seed = uint32(0x1A2B3C4D)

	for _, split := range []int{0, 1, 8, 20, len(full)} {
		got := crc32c(crc32c(seed, full[:split]), full[split:])
		want := crc32c(seed, full)
		assert.Equalf(t, want, got, "chaining identity broke at split=%d", split)
	}
}

// TestCRC32CNoFinalInversion pins the ext4 convention: ext2fs_crc32c_le omits the
// final inversion, so the standard "123456789" check string returns the complement
// of the textbook 0xE3069283 — i.e. 0x1CF96D7C. A double-inverted helper would
// return the textbook value and every on-disk checksum would silently mismatch.
func TestCRC32CNoFinalInversion(t *testing.T) {
	assert.Equal(t, uint32(0x1CF96D7C), crc32c(0xFFFFFFFF, []byte("123456789")))
}

// =============================================================================
// Recipe validation against a real mke2fs image (Task 1b + Task 2)
// =============================================================================

// TestMetadataCsumRecipesMatchRealImage is the empirical de-risking core: it
// builds a real metadata_csum filesystem with mke2fs (our geometry, UUID-derived
// seed) and asserts every per-structure recipe in csum.go reproduces the
// checksum mke2fs already stored — byte-for-byte. This is the authoritative
// tie-breaker for the recipe details (notably the group-descriptor coverage)
// before any of these functions are wired into the writer.
func TestMetadataCsumRecipesMatchRealImage(t *testing.T) {
	data := mkfsMetadataCsumImage(t, 16, "-N 8192")

	sb := data[superblockOffset : superblockOffset+1024]

	// Superblock checksum (Task 1b): plain ~0 seed, coverage sb[0:0x3FC].
	assert.Equal(t, binary.LittleEndian.Uint32(sb[0x3FC:]), superblockCsum(sb), "superblock")

	// This fixture is built with metadata_csum_seed off, so the seed is the
	// UUID-derived value this test reproduces with deriveCsumSeed. (Stored-seed
	// images — metadata_csum_seed on — are reopened and validated by the
	// metadata_csum_seed tests.)
	require.Equal(t, uint32(0), binary.LittleEndian.Uint32(sb[0x270:]), "s_checksum_seed must be 0 (UUID-derived)")
	require.Equal(t, uint8(checksumTypeCRC32C), sb[0x175], "checksum type must be crc32c")

	fsSeed := deriveCsumSeed(sb[0x68 : 0x68+16])
	ipg := binary.LittleEndian.Uint32(sb[0x28:])

	// Group descriptor 0 sits at the start of the GDT (block 1 for 4096 blocks).
	gd0 := data[blockSize : blockSize+32]
	bblk := binary.LittleEndian.Uint32(gd0[0:])
	iblk := binary.LittleEndian.Uint32(gd0[4:])
	itbl := binary.LittleEndian.Uint32(gd0[8:])

	assert.Equal(t, binary.LittleEndian.Uint16(gd0[0x1E:]), groupDescCsum(fsSeed, 0, gd0), "group descriptor")

	// Block bitmap: full 4096-byte block.
	bbOff := uint64(bblk) * blockSize
	assert.Equal(t, binary.LittleEndian.Uint16(gd0[0x18:]),
		bitmapCsum(fsSeed, data[bbOff:bbOff+blockSize]), "block bitmap")

	// Inode bitmap: used portion only = (inodes_per_group+7)/8.
	ibOff := uint64(iblk) * blockSize
	cov := uint64((ipg + 7) / 8)
	assert.Equal(t, binary.LittleEndian.Uint16(gd0[0x1A:]),
		bitmapCsum(fsSeed, data[ibOff:ibOff+cov]), "inode bitmap")

	// Root inode (#2): full 256 bytes, both checksum fields zeroed.
	ino2Off := uint64(itbl)*blockSize + (RootInode-1)*inodeSize
	ino2 := data[ino2Off : ino2Off+inodeSize]
	lo, hi := inodeCsum(fsSeed, RootInode, ino2)
	assert.Equal(t, binary.LittleEndian.Uint16(ino2[0x7C:]), lo, "inode checksum lo")
	assert.Equal(t, binary.LittleEndian.Uint16(ino2[0x82:]), hi, "inode checksum hi")

	// Root directory block: reached via inode #2's first (inline) extent. The
	// directory checksum folds the dir inode's own generation.
	gen := binary.LittleEndian.Uint32(ino2[0x64:])
	extPhys := binary.LittleEndian.Uint32(ino2[0x28+12+8:]) // ee_start_lo of first extent
	dirOff := uint64(extPhys) * blockSize
	dirBlk := data[dirOff : dirOff+blockSize]
	assert.Equal(t, binary.LittleEndian.Uint32(dirBlk[blockSize-4:]),
		dirBlockCsum(fsSeed, RootInode, gen, dirBlk), "directory block")
}

// TestForeignNonzeroGenerationAppend is the only test that drives a nonzero
// i_generation through the writer. Our own writer assigns generation 0 to every
// inode and mke2fs assigns 0 to the root/lost+found, so without a kernel-created
// inode nothing would catch a writer that folded a hardcoded 0 instead of the real
// generation. Here the KERNEL creates /sub (assigning it a nonzero generation),
// then we Open the image and append into /sub — rewriting /sub's inode and mutating
// /sub's directory block — and re-derive both checksums reading the on-disk
// generation. A writer that dropped or hardcoded the generation would store a
// checksum that folds 0 and mismatch here. (The folding recipe itself — le32, at
// the right position — is validated against real mke2fs in
// TestMetadataCsumRecipesMatchRealImage.) The appended image is then handed to
// e2fsck as an independent oracle for the whole result.
func TestForeignNonzeroGenerationAppend(t *testing.T) {
	imgPath := buildForeignImageWithKernelSubdir(t)

	img, err := Open(WithExistingImagePath(imgPath))
	require.NoError(t, err)

	subInode, err := img.builder.findEntry(RootInode, "sub")
	require.NoError(t, err)
	require.NotZero(t, subInode, "kernel-created /sub must be present")

	subIno, err := img.builder.readInode(subInode)
	require.NoError(t, err)
	require.NotZero(t, subIno.Generation, "a kernel-created directory must carry a nonzero i_generation")

	// CreateDirectory increments /sub's link count → rewrites /sub's inode (inode
	// checksum over the nonzero generation) and adds an entry to /sub's directory
	// block (det_checksum folding the nonzero generation).
	_, err = img.CreateDirectory(subInode, "child", 0755, 0, 0)
	require.NoError(t, err)
	_, err = img.CreateFile(subInode, "newfile", []byte("data"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())

	b := img.builder
	seed := b.csumSeed

	// /sub's inode checksum must fold its real (nonzero) generation.
	raw := make([]byte, inodeSize)
	require.NoError(t, b.disk.readAt(raw, int64(b.layout.InodeOffset(subInode))))
	require.NotZero(t, binary.LittleEndian.Uint32(raw[0x64:0x68]), "generation must survive on disk")
	lo, hi := inodeCsum(seed, subInode, raw)
	assert.Equal(t, binary.LittleEndian.Uint16(raw[0x7C:]), lo, "sub inode checksum lo (nonzero gen)")
	assert.Equal(t, binary.LittleEndian.Uint16(raw[0x82:]), hi, "sub inode checksum hi (nonzero gen)")

	// /sub's directory block tail must fold /sub's real generation.
	subIno, err = b.readInode(subInode)
	require.NoError(t, err)
	blocks, err := b.getInodeBlocks(subIno)
	require.NoError(t, err)
	for _, blk := range blocks {
		buf := make([]byte, blockSize)
		require.NoError(t, b.disk.readAt(buf, int64(b.layout.BlockOffset(blk))))
		assert.Equal(t, binary.LittleEndian.Uint32(buf[blockSize-4:]),
			dirBlockCsum(seed, subInode, subIno.Generation, buf), "sub dir block checksum (nonzero gen)")
	}

	require.NoError(t, img.Close())

	requireE2fsckClean(t, imgPath)
}

// requireDocker skips the test unless the Docker daemon answers within a short
// deadline (a stuck daemon must skip cleanly, not hang the suite).
func requireDocker(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		t.Skip("docker not available")
	}
}

// dockerRunScript runs `sh -c script` in a throwaway alpine container with dir
// bind-mounted at /work, under a deadline so a hung daemon, stalled apk, or stuck
// e2fsck cannot hang the whole run. It returns the combined output and run error.
func dockerRunScript(dir, script string, privileged bool) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	args := []string{"run", "--rm"}
	if privileged {
		args = append(args, "--privileged")
	}
	args = append(args, "-v", dir+":/work", "alpine:3.21", "sh", "-c", script)

	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("docker run: %w", err)
	}

	return out, nil
}

// buildForeignImageWithKernelSubdir builds a foreign metadata_csum image of our
// geometry, mounts it, and has the KERNEL create /sub — which assigns a nonzero
// i_generation our own writer never produces — then unmounts, leaving a clean
// fixture. It returns the host image path. It skips the test when Docker (with
// privileged loop-mount) or e2fsprogs is unavailable. `set -eu` fails the fixture
// on the first error; the final chmod lets a non-root host (CI) open the
// root-owned image for read-write.
func buildForeignImageWithKernelSubdir(t *testing.T) (imgPath string) {
	t.Helper()
	requireDocker(t)

	dir, err := os.MkdirTemp("", "ext4fs-gen-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	script := `set -eu
apk add --no-cache e2fsprogs e2fsprogs-extra >/dev/null 2>&1 || exit 3
dd if=/dev/zero of=/work/img bs=1M count=64 status=none
mkfs.ext4 -F -q -b 4096 -I 256 -g 32768 -N 8192 -O ^64bit,^flex_bg,^has_journal,^resize_inode,^metadata_csum_seed,metadata_csum,extent,sparse_super,dir_index,filetype,extra_isize /work/img
mkdir -p /mnt/t
mount -t ext4 -o loop /work/img /mnt/t
mkdir /mnt/t/sub
echo hi > /mnt/t/sub/seed
umount /mnt/t
chmod 666 /work/img
`

	if out, err := dockerRunScript(dir, script, true); err != nil {
		t.Skipf("kernel-subdir fixture unavailable (infra): %v\n%s", err, out)
	}

	return filepath.Join(dir, "img")
}

// requireE2fsckClean runs e2fsck -fn on the image at imgPath inside a throwaway
// container and fails unless it exits 0 (no errors).
func requireE2fsckClean(t *testing.T, imgPath string) {
	t.Helper()

	out, _ := dockerRunScript(filepath.Dir(imgPath),
		fmt.Sprintf("apk add --no-cache e2fsprogs >/dev/null 2>&1 || exit 3; e2fsck -fn /work/%s; echo EXIT=$?",
			filepath.Base(imgPath)), false)

	require.Contains(t, string(out), "EXIT=0", "e2fsck must pass clean:\n%s", out)
}

// mkfsMetadataCsumImage builds a real ext4 metadata_csum image with mke2fs in a
// throwaway Alpine container and returns its raw bytes. The geometry mirrors this
// library's (4096 block, 256 inode, 32-byte descriptors, no 64bit/flex_bg) and
// metadata_csum_seed is disabled so the seed is UUID-derived. It skips the test
// when Docker or e2fsprogs is unavailable, so a missing toolchain never fails the
// suite.
func mkfsMetadataCsumImage(t *testing.T, sizeMB int, extraFlags string) []byte {
	t.Helper()
	requireDocker(t)

	dir, err := os.MkdirTemp("", "ext4fs-csum-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	script := fmt.Sprintf(`set -eu
apk add --no-cache e2fsprogs >/dev/null 2>&1 || exit 3
dd if=/dev/zero of=/work/img bs=1M count=%d status=none
mkfs.ext4 -F -b 4096 -I 256 -g 32768 %s -O ^64bit,^flex_bg,^has_journal,^resize_inode,^metadata_csum_seed,metadata_csum,extent,sparse_super,dir_index,filetype,extra_isize /work/img >/dev/null 2>&1
chmod 666 /work/img
`, sizeMB, extraFlags)

	if out, err := dockerRunScript(dir, script, false); err != nil {
		t.Skipf("mke2fs container unavailable (infra): %v\n%s", err, out)
	}

	data, err := os.ReadFile(filepath.Join(dir, "img"))
	require.NoError(t, err)
	require.Greater(t, len(data), 2048)
	require.Equal(t, uint16(ext4Magic), binary.LittleEndian.Uint16(data[superblockOffset+0x38:]),
		"mke2fs output missing ext4 magic")

	return data
}
