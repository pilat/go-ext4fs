package ext4fs

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReopenDetectsChecksum verifies that reopening a metadata_csum image sets
// csumEnabled and re-derives the FS-wide seed from the on-disk UUID, so further
// writes keep the checksums valid across the open boundary.
func TestReopenDetectsChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "csum.img")

	img, err := New(WithImagePath(path), WithSizeInMB(16), WithChecksum())
	require.NoError(t, err)

	createdSeed := img.builder.csumSeed
	require.NotZero(t, createdSeed)

	_, err = img.CreateFile(RootInode, "a", []byte("x"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	reopened, err := Open(WithExistingImagePath(path))
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()

	assert.True(t, reopened.builder.csumEnabled, "reopen must detect metadata_csum")
	assert.Equal(t, createdSeed, reopened.builder.csumSeed, "reopen seed must match create-time seed")

	sb := make([]byte, 1024)
	require.NoError(t, reopened.builder.disk.readAt(sb, superblockOffset))
	assert.Equal(t, deriveCsumSeed(sb[0x68:0x78]), reopened.builder.csumSeed,
		"reopen seed must equal crc32c(~0, uuid)")
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(sb[0x270:]),
		"our images never set s_checksum_seed")
}

// TestReopenNonChecksumImage verifies the default (no-checksum) image reopens with
// checksums off, so the byte-for-byte default path is never accidentally upgraded.
func TestReopenNonChecksumImage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plain.img")

	img, err := New(WithImagePath(path), WithSizeInMB(16))
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())

	reopened, err := Open(WithExistingImagePath(path))
	require.NoError(t, err)
	defer func() { _ = reopened.Close() }()

	assert.False(t, reopened.builder.csumEnabled)
	assert.Zero(t, reopened.builder.csumSeed)
}
