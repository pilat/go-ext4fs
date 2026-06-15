package ext4fs

import "math/bits"

// Directory hashing for ext4 hash-tree (htree) directories.
//
// This is a byte-exact port of the kernel's half_md4 directory hash
// (fs/ext4/hash.c __ext4fs_dirhash) and e2fsprogs (lib/ext2fs/dirhash.c).
// htree validity depends on hashing each name with the SAME algorithm, seed and
// signedness the on-disk filesystem advertises: a wrong hash still lets readdir
// list every file, but makes open()-by-name silently miss. It must therefore be
// bit-identical to the reference, which is why it is validated directly against
// mke2fs/e2fsprogs output (see the Task-2 de-risk tests).
//
// Only the half_md4 variants (signed and unsigned) are implemented — that is the
// mke2fs and kernel default. Legacy and TEA hashes are intentionally unsupported;
// the foreign-image path refuses to reindex a directory hashed with anything else
// rather than silently producing a wrong hash.

const (
	// htreeEOF32Blk mirrors EXT4_HTREE_EOF_32BLK = (1 << 31) - 1. A computed
	// hash that collides with the shifted EOF sentinel is nudged down by one,
	// exactly as the kernel does, so it can never be mistaken for end-of-tree.
	htreeEOF32Blk = uint32(1)<<31 - 1

	// MD4 round constants. In the kernel these are the octal literals
	// K2 = 013240474631 and K3 = 015666365641.
	md4K1 = 0x00000000
	md4K2 = 0x5A827999
	md4K3 = 0x6ED9EBA1
)

// hashVersionSupported reports whether ext4Dirhash can hash with the given
// (signedness-resolved) version. Only the half_md4 family is supported.
func hashVersionSupported(version uint8) bool {
	return version == hashVersionHalfMD4 || version == hashVersionHalfMD4Unsigned
}

// effectiveHashVersion applies the kernel's signedness adjustment: for the
// legacy/half_md4/tea family (version <= DX_HASH_TEA == 2) an unsigned-hash
// filesystem selects the *_UNSIGNED variant by adding 3. A version already >= 3
// (already an unsigned variant) is returned unchanged, matching the kernel's
// `hash_version <= DX_HASH_TEA` guard.
func effectiveHashVersion(version uint8, unsigned bool) uint8 {
	if unsigned && version <= 2 {
		return version + 3
	}
	return version
}

// md4F, md4G, md4H are the MD4 selection, majority and parity functions.
func md4F(x, y, z uint32) uint32 { return z ^ (x & (y ^ z)) }
func md4G(x, y, z uint32) uint32 { return (x & y) + ((x ^ y) & z) }
func md4H(x, y, z uint32) uint32 { return x ^ y ^ z }

// halfMD4Transform is the cut-down MD4 transform: it mixes the 8-word block in
// into the 4-word state buf (updated in place) and returns buf[1]. Ported from
// the kernel half_md4_transform; the round schedule and shift amounts are part
// of the on-disk contract and must not change.
func halfMD4Transform(buf *[4]uint32, in *[8]uint32) {
	a, b, c, d := buf[0], buf[1], buf[2], buf[3]

	round := func(f func(uint32, uint32, uint32) uint32, acc *uint32, x, y, z, in uint32, s int) {
		*acc += f(x, y, z) + in
		*acc = bits.RotateLeft32(*acc, s)
	}

	// Round 1 (F)
	round(md4F, &a, b, c, d, in[0]+md4K1, 3)
	round(md4F, &d, a, b, c, in[1]+md4K1, 7)
	round(md4F, &c, d, a, b, in[2]+md4K1, 11)
	round(md4F, &b, c, d, a, in[3]+md4K1, 19)
	round(md4F, &a, b, c, d, in[4]+md4K1, 3)
	round(md4F, &d, a, b, c, in[5]+md4K1, 7)
	round(md4F, &c, d, a, b, in[6]+md4K1, 11)
	round(md4F, &b, c, d, a, in[7]+md4K1, 19)

	// Round 2 (G)
	round(md4G, &a, b, c, d, in[1]+md4K2, 3)
	round(md4G, &d, a, b, c, in[3]+md4K2, 5)
	round(md4G, &c, d, a, b, in[5]+md4K2, 9)
	round(md4G, &b, c, d, a, in[7]+md4K2, 13)
	round(md4G, &a, b, c, d, in[0]+md4K2, 3)
	round(md4G, &d, a, b, c, in[2]+md4K2, 5)
	round(md4G, &c, d, a, b, in[4]+md4K2, 9)
	round(md4G, &b, c, d, a, in[6]+md4K2, 13)

	// Round 3 (H)
	round(md4H, &a, b, c, d, in[3]+md4K3, 3)
	round(md4H, &d, a, b, c, in[7]+md4K3, 9)
	round(md4H, &c, d, a, b, in[2]+md4K3, 11)
	round(md4H, &b, c, d, a, in[6]+md4K3, 15)
	round(md4H, &a, b, c, d, in[1]+md4K3, 3)
	round(md4H, &d, a, b, c, in[5]+md4K3, 9)
	round(md4H, &c, d, a, b, in[0]+md4K3, 11)
	round(md4H, &b, c, d, a, in[4]+md4K3, 15)

	buf[0] += a
	buf[1] += b
	buf[2] += c
	buf[3] += d
}

// str2hashbuf packs up to num 32-bit words from the first min(len, num*4) bytes
// of msg into buf, padding the remainder. It is the kernel's
// str2hashbuf_{signed,unsigned}; the only difference between the two variants is
// whether each input byte is sign-extended (signed) or zero-extended (unsigned),
// which is also the only thing that makes the two hashes differ — and only for
// bytes >= 0x80. length is the full remaining name length (used to derive the
// pad), even when it exceeds num*4.
func str2hashbuf(msg []byte, length int, buf []uint32, num int, signed bool) {
	pad := uint32(length) | uint32(length)<<8
	pad |= pad << 16

	val := pad
	if length > num*4 {
		length = num * 4
	}

	j := 0
	for i := 0; i < length; i++ {
		var c uint32
		if signed {
			c = uint32(int32(int8(msg[i]))) // sign-extend, then reinterpret
		} else {
			c = uint32(msg[i]) // zero-extend
		}
		val = c + (val << 8)
		if i%4 == 3 {
			buf[j] = val
			j++
			val = pad
			num--
		}
	}

	num--
	if num >= 0 {
		buf[j] = val
		j++
	}
	for {
		num--
		if num < 0 {
			break
		}
		buf[j] = pad
		j++
	}
}

// ext4Dirhash computes the (major, minor) directory hash for name using half_md4.
//
// seed is the filesystem's s_hash_seed; an all-zero seed falls back to the
// built-in defaultHashSeed (matching the kernel). version selects signedness:
// hashVersionHalfMD4 (1) hashes signed, hashVersionHalfMD4Unsigned (4) hashes
// unsigned — use effectiveHashVersion to resolve it from a base version and the
// filesystem's signedness flag. The returned major hash has its low bit cleared
// (the htree "hash continues" flag lives there) and avoids the EOF sentinel,
// exactly as the reference does; the minor hash is returned verbatim.
func ext4Dirhash(name []byte, seed [4]uint32, version uint8) (hash, minorHash uint32) {
	buf := defaultHashSeed
	for i := 0; i < 4; i++ {
		if seed[i] != 0 {
			buf = seed
			break
		}
	}

	signed := version != hashVersionHalfMD4Unsigned

	var in [8]uint32
	offset, length := 0, len(name)
	for length > 0 {
		str2hashbuf(name[offset:], length, in[:], 8, signed)
		halfMD4Transform(&buf, &in)
		length -= 32
		offset += 32
	}

	hash = buf[1]
	minorHash = buf[2]

	hash &^= 1
	if hash == htreeEOF32Blk<<1 {
		hash = (htreeEOF32Blk - 1) << 1
	}
	return hash, minorHash
}
