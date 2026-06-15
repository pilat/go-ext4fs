package ext4fs

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadLayoutRejectsUnsafeReopen verifies, without Docker, that
// loadLayoutFromDisk refuses to reopen an image it cannot correctly rewrite:
// reserved GDT blocks (online-resize layout shifts every per-group offset) and any
// RO_COMPAT feature outside the whitelist (e.g. gdt_csum). Benign RO_COMPAT bits
// that only permit a format we never emit (huge_file, dir_nlink) must still open.
func TestLoadLayoutRejectsUnsafeReopen(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(8))
	require.NoError(t, err)
	require.NoError(t, img.Save())

	backend := img.builder.disk

	base := make([]byte, 1024)
	require.NoError(t, backend.readAt(base, superblockOffset))

	patched := func(mutate func([]byte)) {
		sb := append([]byte(nil), base...)
		mutate(sb)
		require.NoError(t, backend.writeAt(sb, superblockOffset))
	}

	t.Run("baseline opens", func(t *testing.T) {
		patched(func(_ []byte) {})
		_, err := loadLayoutFromDisk(backend)
		require.NoError(t, err)
	})

	t.Run("rejects reserved GDT blocks", func(t *testing.T) {
		patched(func(sb []byte) { binary.LittleEndian.PutUint16(sb[0xCE:], 3) })
		_, err := loadLayoutFromDisk(backend)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reserved GDT")
	})

	t.Run("rejects unsupported RO_COMPAT (gdt_csum)", func(t *testing.T) {
		patched(func(sb []byte) {
			ro := binary.LittleEndian.Uint32(sb[0x64:]) | 0x10 // gdt_csum
			binary.LittleEndian.PutUint32(sb[0x64:], ro)
		})
		_, err := loadLayoutFromDisk(backend)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only-compat")
	})

	t.Run("rejects unsupported RO_COMPAT (bigalloc)", func(t *testing.T) {
		patched(func(sb []byte) {
			ro := binary.LittleEndian.Uint32(sb[0x64:]) | 0x200 // bigalloc
			binary.LittleEndian.PutUint32(sb[0x64:], ro)
		})
		_, err := loadLayoutFromDisk(backend)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "read-only-compat")
	})

	t.Run("tolerates benign RO_COMPAT (huge_file, dir_nlink)", func(t *testing.T) {
		patched(func(sb []byte) {
			ro := binary.LittleEndian.Uint32(sb[0x64:]) | roCompatHugeFile | roCompatDirNlink
			binary.LittleEndian.PutUint32(sb[0x64:], ro)
		})
		_, err := loadLayoutFromDisk(backend)
		require.NoError(t, err, "huge_file/dir_nlink only permit formats we never emit")
	})
}
