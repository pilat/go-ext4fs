package ext4fs

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roCompatFieldOffset is the byte offset of s_feature_ro_compat: the primary
// superblock lives at superblockOffset (1024) and the field sits at 0x64 within it.
const roCompatFieldOffset = superblockOffset + 0x64

// defHashVersionOffset is the byte offset of s_def_hash_version (0xFC within the
// primary superblock).
const defHashVersionOffset = superblockOffset + 0xFC

// buildOwnImage creates a minimal image with this library and returns its path.
// Our own images carry no metadata_csum, so byte-patching the superblock afterward
// needs no checksum fix-up to remain openable.
func buildOwnImage(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "own.img")
	img, err := New(WithImagePath(path), WithSizeInMB(8))
	require.NoError(t, err)
	_, err = img.CreateFile(RootInode, "hello.txt", []byte("hi\n"), 0644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, img.Save())
	require.NoError(t, img.Close())
	return path
}

// orROCompatBit sets bit in the primary superblock's s_feature_ro_compat field.
func orROCompatBit(t *testing.T, path string, bit uint32) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	buf := make([]byte, 4)
	_, err = f.ReadAt(buf, roCompatFieldOffset)
	require.NoError(t, err)
	binary.LittleEndian.PutUint32(buf, binary.LittleEndian.Uint32(buf)|bit)
	_, err = f.WriteAt(buf, roCompatFieldOffset)
	require.NoError(t, err)
}

// TestOpenRejectsUnsupportedROCompat is the unit guard for the ro_compat allowlist:
// an image patched to advertise a feature Save cannot maintain (gdt_csum rewrites
// per-descriptor checksums; bigalloc turns the block bitmap into a cluster bitmap)
// must be refused on Open, named in the error, rather than silently corrupted on
// the next Save.
func TestOpenRejectsUnsupportedROCompat(t *testing.T) {
	cases := []struct {
		name    string
		bit     uint32
		wantErr string
	}{
		{"gdt_csum", roCompatGdtCsum, "uninit_bg/gdt_csum"},
		{"bigalloc", roCompatBigalloc, "bigalloc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := buildOwnImage(t)
			orROCompatBit(t, path, tc.bit)
			_, err := Open(WithExistingImagePath(path))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// TestOpenAcceptsSafeROCompat guards against over-rejection: a purely descriptive
// ro_compat bit real mke2fs sets (huge_file) must keep the image openable.
func TestOpenAcceptsSafeROCompat(t *testing.T) {
	path := buildOwnImage(t)
	orROCompatBit(t, path, roCompatHugeFile)
	img, err := Open(WithExistingImagePath(path))
	require.NoError(t, err)
	require.NoError(t, img.Close())
}

// TestOpenDetectsUnsignedHashFromVersion: ext4 marks the unsigned half_md4 variant
// either via the s_flags UNSIGNED bit OR via s_def_hash_version == *_UNSIGNED. An
// image using the version encoding (no s_flags bit, as our own images leave it)
// must still be treated as unsigned, otherwise the next htree rebuild rehashes it
// as signed and the index stops resolving by name.
func TestOpenDetectsUnsignedHashFromVersion(t *testing.T) {
	path := buildOwnImage(t)

	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{hashVersionHalfMD4Unsigned}, defHashVersionOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	img, err := Open(WithExistingImagePath(path))
	require.NoError(t, err)
	defer func() { _ = img.Close() }()
	assert.False(t, img.builder.signedHash,
		"version-encoded unsigned hash (s_def_hash_version==4) must yield signedHash=false")
}
