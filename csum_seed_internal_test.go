package ext4fs

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadLayoutSelectsCsumSeedSource verifies, without Docker, that
// loadLayoutFromDisk reads the seed from s_checksum_seed when metadata_csum_seed
// is set and derives it from the UUID otherwise.
func TestLoadLayoutSelectsCsumSeedSource(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(8))
	require.NoError(t, err)
	require.NoError(t, img.Save())

	backend := img.builder.disk

	sb := make([]byte, 1024)
	require.NoError(t, backend.readAt(sb, superblockOffset))

	// Mark the image metadata_csum and patch in a sentinel stored seed distinct
	// from the UUID-derived value.
	const sentinel = uint32(0xCAFEBABE)
	require.NotEqual(t, sentinel, deriveCsumSeed(sb[0x68:0x78]), "sentinel must differ from the UUID-derived seed")

	rocompat := binary.LittleEndian.Uint32(sb[0x64:]) | roCompatMetadataCsum
	binary.LittleEndian.PutUint32(sb[0x64:], rocompat)
	binary.LittleEndian.PutUint32(sb[0x270:], sentinel)

	t.Run("stored seed when metadata_csum_seed set", func(t *testing.T) {
		patched := append([]byte(nil), sb...)
		binary.LittleEndian.PutUint32(patched[0x60:], binary.LittleEndian.Uint32(patched[0x60:])|incompatCsumSeed)
		require.NoError(t, backend.writeAt(patched, superblockOffset))

		layout, err := loadLayoutFromDisk(backend)
		require.NoError(t, err)
		assert.True(t, layout.CsumEnabled)
		assert.Equal(t, sentinel, layout.CsumSeed, "must use the stored s_checksum_seed")
	})

	t.Run("derived seed when metadata_csum_seed clear", func(t *testing.T) {
		patched := append([]byte(nil), sb...)
		binary.LittleEndian.PutUint32(patched[0x60:], binary.LittleEndian.Uint32(patched[0x60:])&^incompatCsumSeed)
		require.NoError(t, backend.writeAt(patched, superblockOffset))

		layout, err := loadLayoutFromDisk(backend)
		require.NoError(t, err)
		assert.True(t, layout.CsumEnabled)
		assert.Equal(t, deriveCsumSeed(patched[0x68:0x78]), layout.CsumSeed, "must derive the seed from the UUID")
	})
}

// TestReopenCsumSeedDecoupledFromUUID is the load-bearing test for
// metadata_csum_seed support. mke2fs enables metadata_csum_seed by default with
// metadata_csum; tune2fs -U then changes the UUID WITHOUT touching the stored seed
// (that is the whole point of the feature), so crc32c(~0, uuid) no longer equals
// the real seed. We Open the image, append, and Save. If we derived the seed from
// the UUID (wrong), every checksum we rewrite would mismatch and e2fsck would
// reject; passing proves we used the stored seed.
func TestReopenCsumSeedDecoupledFromUUID(t *testing.T) {
	imgPath := buildCsumSeedImageWithChangedUUID(t)

	img, err := Open(WithExistingImagePath(imgPath))
	require.NoError(t, err, "a metadata_csum_seed image must open")

	b := img.builder
	require.True(t, b.csumEnabled)

	sb := make([]byte, 1024)
	require.NoError(t, b.disk.readAt(sb, superblockOffset))

	storedSeed := binary.LittleEndian.Uint32(sb[0x270:0x274])
	derivedSeed := deriveCsumSeed(sb[0x68:0x78])

	require.NotEqual(t, derivedSeed, storedSeed, "fixture must have a UUID-decoupled seed (tune2fs -U)")
	assert.Equal(t, storedSeed, b.csumSeed, "builder must use the stored seed, not the UUID-derived one")

	_, err = img.CreateFile(RootInode, "appended.txt", []byte("hello\n"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	requireE2fsckClean(t, imgPath)
}

// buildCsumSeedImageWithChangedUUID builds a metadata_csum image of our geometry
// (mke2fs enables metadata_csum_seed by default), then changes the UUID with
// tune2fs so the stored checksum seed is decoupled from the UUID. It returns the
// host image path and skips when Docker/e2fsprogs is unavailable.
func buildCsumSeedImageWithChangedUUID(t *testing.T) (imgPath string) {
	t.Helper()
	requireDocker(t)

	dir, err := os.MkdirTemp("", "ext4fs-seed-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	script := `set -eu
apk add --no-cache e2fsprogs e2fsprogs-extra >/dev/null 2>&1 || exit 3
dd if=/dev/zero of=/work/img bs=1M count=64 status=none
mkfs.ext4 -F -q -b 4096 -I 256 -g 32768 -N 8192 -O ^64bit,^flex_bg,^has_journal,^resize_inode,metadata_csum,extent,sparse_super,dir_index,filetype,extra_isize /work/img
tune2fs -U 11111111-2222-3333-4444-555555555555 /work/img >/dev/null 2>&1
chmod 666 /work/img
`

	if out, err := dockerRunScript(dir, script, false); err != nil {
		t.Skipf("metadata_csum_seed fixture unavailable (infra): %v\n%s", err, out)
	}

	return filepath.Join(dir, "img")
}
