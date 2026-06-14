package ext4fs

import "testing"

// Reference vectors captured from e2fsprogs 1.47.3 by calling the library
// function directly:
//
//	ext2fs_dirhash(version, name, len, NULL /*default seed*/, &major, &minor)
//
// for version 1 (half_md4, signed) and version 4 (half_md4_unsigned). The major
// hash already has its low bit cleared, matching ext4Dirhash.
//
// NOTE: do NOT use debugfs's `dx_hash -h half_md4_unsigned` command as an oracle
// for the unsigned variant — in 1.47.3 it returns values that violate the
// ASCII-identity invariant (it disagrees with signed even for pure-ASCII names,
// where the two MUST be identical). The library ext2fs_dirhash used here is
// authoritative and agrees with ext4Dirhash for both variants. The long-ASCII
// vector pins ASCII-identity: signed and unsigned major+minor are equal.
func TestExt4DirhashReferenceVectors(t *testing.T) {
	var zeroSeed [4]uint32

	vectors := []struct {
		name           string
		bytes          []byte
		sMajor, sMinor uint32 // version 1, half_md4 (signed)
		uMajor, uMinor uint32 // version 4, half_md4_unsigned
	}{
		{"a", []byte("a"), 0xd5fa7d7a, 0xacb48187, 0xd5fa7d7a, 0xacb48187},
		{"hello", []byte("hello"), 0x1746da32, 0x420013b5, 0x1746da32, 0x420013b5},
		{"file_0000.txt", []byte("file_0000.txt"), 0x951a261c, 0x8d3c17f1, 0x951a261c, 0x8d3c17f1},
		{
			"long>32bytes",
			[]byte("this_is_a_long_filename_exceeding_thirty_two_bytes_for_sure.txt"),
			0x9a677aec, 0x6c1fdc27, // signed == unsigned (pure ASCII, multi-block)
			0x9a677aec, 0x6c1fdc27,
		},
		{"éäü", []byte{0xC3, 0xA9, 0xC3, 0xA4, 0xC3, 0xBC}, 0x05b0d50c, 0x3bebed02, 0x052165ae, 0xbb4260ce},
		{"café", []byte{0x63, 0x61, 0x66, 0xC3, 0xA9}, 0xfb9c5e5c, 0x0573e8b8, 0x9d72aed6, 0xf6138c6a},
		{"0xFFFE80", []byte{0xFF, 0xFE, 0x80}, 0x45b5f8b0, 0x1c2ec767, 0x6542b330, 0xc25ed839},
	}

	for _, tc := range vectors {
		t.Run("signed/"+tc.name, func(t *testing.T) {
			h, m := ext4Dirhash(tc.bytes, zeroSeed, hashVersionHalfMD4)
			if h != tc.sMajor || m != tc.sMinor {
				t.Errorf("got (0x%08x,0x%08x), want (0x%08x,0x%08x)", h, m, tc.sMajor, tc.sMinor)
			}
		})
		t.Run("unsigned/"+tc.name, func(t *testing.T) {
			h, m := ext4Dirhash(tc.bytes, zeroSeed, hashVersionHalfMD4Unsigned)
			if h != tc.uMajor || m != tc.uMinor {
				t.Errorf("got (0x%08x,0x%08x), want (0x%08x,0x%08x)", h, m, tc.uMajor, tc.uMinor)
			}
		})
	}
}

// TestExt4DirhashSignedUnsignedDiverge proves both signedness paths are real,
// distinct implementations: for a name with bytes >= 0x80 the signed and
// unsigned hashes must differ. For pure-ASCII names (all bytes < 0x80) the two
// are bit-identical, which this also asserts to pin the boundary.
func TestExt4DirhashSignedUnsignedDiverge(t *testing.T) {
	var zeroSeed [4]uint32

	highByte := []byte{0xC3, 0xA9, 0xC3, 0xA4, 0xC3, 0xBC} // "éäü"
	hs, _ := ext4Dirhash(highByte, zeroSeed, hashVersionHalfMD4)
	hu, _ := ext4Dirhash(highByte, zeroSeed, hashVersionHalfMD4Unsigned)
	if hs == hu {
		t.Errorf("signed and unsigned hashes must differ for high-byte names, both = 0x%08x", hs)
	}

	ascii := []byte("plain_ascii_name.txt")
	as, _ := ext4Dirhash(ascii, zeroSeed, hashVersionHalfMD4)
	au, _ := ext4Dirhash(ascii, zeroSeed, hashVersionHalfMD4Unsigned)
	if as != au {
		t.Errorf("signed and unsigned hashes must match for pure-ASCII names: signed=0x%08x unsigned=0x%08x", as, au)
	}
}

// TestExt4DirhashSeedFallback verifies that an all-zero seed falls back to the
// built-in default (so an explicit defaultHashSeed and a zero seed agree) and
// that a different non-zero seed changes the hash.
func TestExt4DirhashSeedFallback(t *testing.T) {
	name := []byte("seedcheck")

	var zeroSeed [4]uint32
	hz, mz := ext4Dirhash(name, zeroSeed, hashVersionHalfMD4)
	hd, md := ext4Dirhash(name, defaultHashSeed, hashVersionHalfMD4)
	if hz != hd || mz != md {
		t.Errorf("zero seed must fall back to default: zero=(0x%08x,0x%08x) default=(0x%08x,0x%08x)", hz, mz, hd, md)
	}

	other := [4]uint32{1, 2, 3, 4}
	ho, _ := ext4Dirhash(name, other, hashVersionHalfMD4)
	if ho == hz {
		t.Errorf("a non-zero seed must change the hash, got 0x%08x for both", ho)
	}
}

// TestEffectiveHashVersion pins the kernel signedness adjustment.
func TestEffectiveHashVersion(t *testing.T) {
	cases := []struct {
		version  uint8
		unsigned bool
		want     uint8
	}{
		{hashVersionHalfMD4, false, hashVersionHalfMD4},        // signed half_md4 stays 1
		{hashVersionHalfMD4, true, hashVersionHalfMD4Unsigned}, // unsigned half_md4 -> 4
		{2, true, 5}, // tea -> tea_unsigned
		{hashVersionHalfMD4Unsigned, true, hashVersionHalfMD4Unsigned}, // already unsigned, no double-bump
		{4, false, 4}, // unsigned variant, signed flag off
	}
	for _, c := range cases {
		if got := effectiveHashVersion(c.version, c.unsigned); got != c.want {
			t.Errorf("effectiveHashVersion(%d, %v) = %d, want %d", c.version, c.unsigned, got, c.want)
		}
	}
}
