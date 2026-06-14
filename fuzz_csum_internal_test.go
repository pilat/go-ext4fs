package ext4fs

import (
	"bytes"
	"path/filepath"
	"testing"
)

// FuzzChecksumOps drives a metadata_csum image through an arbitrary operation
// sequence and asserts, after every Save, the two invariants no input may break:
// every metadata checksum is self-consistent (re-deriving from the on-disk bytes
// matches what was stored) and the free-block counts match the bitmaps. It runs
// the sequence once on a fresh image, then again after Open — covering the seed
// re-derivation and freed-hole accounting on the reopen path. Individual ops are
// free to fail (invalid names, missing targets, exhausted space, and the
// guarded xattr/extent paths); none may corrupt the checksummed state.
func FuzzChecksumOps(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 16, 0})
	f.Add([]byte{1, 0, 1, 0, 1, 2, 0, 1})
	f.Add([]byte{2, 0, 1, 0, 0, 0, 8, 0, 3, 0, 1})
	f.Add([]byte{0, 0, 0, 255, 255, 2, 0, 0, 1, 0, 1, 0, 0, 1, 16, 0})
	f.Add([]byte{4, 0, 2, 3, 6, 0, 4, 0, 1, 0, 5, 0, 0, 0, 255, 255})

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.img")

		img, err := New(WithImagePath(path), WithSizeInMB(8), WithChecksum())
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		applyChecksumFuzzOps(img, data)

		if err := img.Save(); err != nil {
			t.Fatalf("Save: %v", err)
		}
		validateChecksumsSelfConsistent(t, img)
		assertFreeBlockCountsMatchBitmaps(t, img)
		if err := img.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		img2, err := Open(WithExistingImagePath(path))
		if err != nil {
			t.Fatalf("reopen of just-saved image: %v", err)
		}
		if !img2.builder.csumEnabled {
			t.Fatal("reopened metadata_csum image lost csumEnabled")
		}

		applyChecksumFuzzOps(img2, data)

		if err := img2.Save(); err != nil {
			t.Fatalf("Save after reopen: %v", err)
		}
		validateChecksumsSelfConsistent(t, img2)
		assertFreeBlockCountsMatchBitmaps(t, img2)
		if err := img2.Close(); err != nil {
			t.Fatalf("Close after reopen: %v", err)
		}
	})
}

// applyChecksumFuzzOps interprets data as an operation sequence (one byte selects
// the op, following bytes parameterize it) against the public API. Operation
// errors are deliberately ignored — invalid inputs and guarded operations must be
// rejected gracefully, never corrupt state.
func applyChecksumFuzzOps(img *Image, data []byte) {
	names := [8]string{"n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7"}
	dirs := []uint32{RootInode}
	var files []uint32

	for i := 0; i < len(data); {
		op := data[i]
		i++

		arg := func() byte {
			if i >= len(data) {
				return 0
			}
			b := data[i]
			i++
			return b
		}

		parent := dirs[int(arg())%len(dirs)]
		name := names[int(arg())%len(names)]

		switch op % 8 {
		case 0: // create a file of up to 64 KiB
			size := (int(arg())<<8 | int(arg())) % (64 << 10)
			if ino, err := img.CreateFile(parent, name, bytes.Repeat([]byte{op}, size), 0644, 0, 0); err == nil {
				files = append(files, ino)
			}
		case 1:
			if ino, err := img.CreateDirectory(parent, name, 0755, 0, 0); err == nil {
				dirs = append(dirs, ino)
			}
		case 2:
			_ = img.Delete(parent, name)
		case 3:
			_ = img.DeleteDirectory(parent, name)
		case 4:
			_, _ = img.CreateSymlink(parent, name, "/"+names[int(arg())%len(names)], 0, 0)
		case 5: // guarded under metadata_csum; must error, never corrupt
			_ = img.SetXattr(parent, "user."+name, bytes.Repeat([]byte{op}, int(arg())))
		case 6:
			if len(files) > 0 {
				_ = img.Link(parent, name, files[int(arg())%len(files)])
			}
		case 7: // raw, possibly invalid name
			end := i + int(arg())%32 + 1
			if end > len(data) {
				end = len(data)
			}
			_, _ = img.CreateFile(parent, string(data[i:end]), []byte("x"), 0644, 0, 0)
			i = end
		}
	}
}
