package ext4fs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDirRecLen(t *testing.T) {
	cases := []struct {
		nameLen int
		want    int
	}{
		{1, 12}, {2, 12}, {3, 12}, {4, 12}, // 8+name rounded up to 4
		{5, 16}, {8, 16}, {9, 20},
		{255, 264}, // max name: roundup(263,4)
	}
	for _, c := range cases {
		if got := dirRecLen(c.nameLen); got != c.want {
			t.Errorf("dirRecLen(%d) = %d, want %d", c.nameLen, got, c.want)
		}
	}
}

func he(major, minor uint32, name string) hashedEntry {
	return hashedEntry{
		entry: dirEntry{Inode: 11, Type: ftRegFile, Name: []byte(name)},
		major: major, minor: minor,
	}
}

func leafNames(leaf htreeLeafPlan) []string {
	out := make([]string, len(leaf.entries))
	for i, e := range leaf.entries {
		out[i] = string(e.Name)
	}
	return out
}

func TestPackHtreeLeavesSingleLeaf(t *testing.T) {
	hes := []hashedEntry{he(10, 1, "a"), he(20, 1, "b"), he(30, 1, "c")}
	leaves, err := packHtreeLeaves(hes, blockSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(leaves) != 1 {
		t.Fatalf("got %d leaves, want 1", len(leaves))
	}
	if leaves[0].boundaryHash != 10 {
		t.Errorf("leaf boundary = %d, want 10 (min major)", leaves[0].boundaryHash)
	}
	if got := leafNames(leaves[0]); len(got) != 3 {
		t.Errorf("leaf entries = %v, want 3", got)
	}
}

// TestPackHtreeLeavesBoundaryAndGrouping verifies leaves split only between
// distinct major hashes and that a same-major group is never split across leaves.
func TestPackHtreeLeavesBoundaryAndGrouping(t *testing.T) {
	// capacity 24 fits exactly two 12-byte dirents. Group major=5 has one entry;
	// group major=10 has two entries (24 bytes). Adding group 10 to a leaf already
	// holding group 5 (12 bytes) would overflow, so group 10 moves whole to leaf 2.
	hes := []hashedEntry{
		he(5, 1, "a"),  // group 5: 1 entry
		he(10, 1, "b"), // group 10: 2 entries, must stay together
		he(10, 2, "c"),
	}
	leaves, err := packHtreeLeaves(hes, 24)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(leaves) != 2 {
		t.Fatalf("got %d leaves, want 2", len(leaves))
	}
	if leaves[0].boundaryHash != 5 || len(leaves[0].entries) != 1 {
		t.Errorf("leaf0 = boundary %d, %v; want boundary 5 with 1 entry", leaves[0].boundaryHash, leafNames(leaves[0]))
	}
	if leaves[1].boundaryHash != 10 {
		t.Errorf("leaf1 boundary = %d, want 10", leaves[1].boundaryHash)
	}
	if got := leafNames(leaves[1]); len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("group major=10 was split or reordered: leaf1 = %v, want [b c]", got)
	}
}

// TestPackHtreeLeavesSingleGroupTooBig: one hash value with more colliding
// entries than fit in a leaf is not representable depth-1 and must return the
// sentinel.
func TestPackHtreeLeavesSingleGroupTooBig(t *testing.T) {
	hes := []hashedEntry{he(7, 1, "a"), he(7, 2, "b"), he(7, 3, "c")} // 36 bytes, all major 7
	_, err := packHtreeLeaves(hes, 24)
	if !errors.Is(err, errHtreeDepth1Exceeded) {
		t.Fatalf("got %v, want errHtreeDepth1Exceeded", err)
	}
}

// TestPackHtreeLeavesManyLeaves: distinct majors each in their own small leaf
// produce one leaf per entry, validating the leaf-count that feeds emit's
// depth-1 bound.
func TestPackHtreeLeavesManyLeaves(t *testing.T) {
	const n = 600
	hes := make([]hashedEntry, n)
	for i := 0; i < n; i++ {
		hes[i] = he(uint32(i*2+2), 1, fmt.Sprintf("n%d", i)) // distinct even majors
	}
	leaves, err := packHtreeLeaves(hes, 16) // one 12-byte dirent per leaf
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(leaves) != n {
		t.Fatalf("got %d leaves, want %d", len(leaves), n)
	}
	// Boundaries must be strictly ascending.
	for i := 1; i < len(leaves); i++ {
		if leaves[i].boundaryHash <= leaves[i-1].boundaryHash {
			t.Fatalf("boundary not ascending at leaf %d: %d <= %d", i, leaves[i].boundaryHash, leaves[i-1].boundaryHash)
		}
	}
}

// dirSnapshot captures everything emitHtree must NOT change on the sentinel path.
type dirSnapshot struct {
	block0                  []byte
	flags, sizeLo, blocksLo uint32
	nextBlock0              uint32
	freeRuns                int
}

func captureDirSnapshot(t *testing.T, img *Image, dInode uint32) dirSnapshot {
	t.Helper()
	b := img.builder
	ino, err := b.readInode(dInode)
	if err != nil {
		t.Fatalf("readInode: %v", err)
	}
	blocks, err := b.getInodeBlocks(ino)
	if err != nil {
		t.Fatalf("getInodeBlocks: %v", err)
	}
	block0, err := img.ReadBlockForTest(blocks[0])
	if err != nil {
		t.Fatalf("read block 0: %v", err)
	}
	flags, sizeLo, blocksLo, _, _, _ := img.InodeFieldsForTest(dInode)
	return dirSnapshot{block0, flags, sizeLo, blocksLo, b.nextBlockPerGroup[0], len(b.freeRuns)}
}

func (before dirSnapshot) assertUnchanged(t *testing.T, after dirSnapshot) {
	t.Helper()
	if !bytes.Equal(before.block0, after.block0) {
		t.Error("block 0 changed after sentinel-rejected emit")
	}
	if after.flags != before.flags || after.flags&inodeFlagIndex != 0 {
		t.Errorf("inode flags changed: 0x%x -> 0x%x", before.flags, after.flags)
	}
	if after.sizeLo != before.sizeLo || after.blocksLo != before.blocksLo {
		t.Errorf("inode size/blocks changed: size %d->%d blocks %d->%d", before.sizeLo, after.sizeLo, before.blocksLo, after.blocksLo)
	}
	if after.nextBlock0 != before.nextBlock0 || after.freeRuns != before.freeRuns {
		t.Errorf("allocation state changed: nextBlock %d->%d freeRuns %d->%d", before.nextBlock0, after.nextBlock0, before.freeRuns, after.freeRuns)
	}
}

// infeasibleEntries returns n distinct 255-char names that pack into more than
// dxRootLimit leaves (~15 per leaf), forcing the depth-1 sentinel.
func infeasibleEntries(n int) []dirEntry {
	longSuffix := strings.Repeat("x", 250)
	entries := make([]dirEntry, n)
	for i := range entries {
		entries[i] = dirEntry{Inode: 11, Type: ftRegFile, Name: []byte(fmt.Sprintf("%04d%s", i, longSuffix))}
	}
	return entries
}

// TestPackHtreeLeavesAtDepth1Limit pins the exact depth-1 bound the emit gate
// (len(leaves) > dxRootLimit) relies on: dxRootLimit is 508, a 508-distinct-hash
// set packs into exactly 508 leaves (accepted), and 509 into 509 (rejected). The
// constant must be exact because e2fsck checks the dx_root limit for equality.
func TestPackHtreeLeavesAtDepth1Limit(t *testing.T) {
	if dxRootLimit != 508 {
		t.Fatalf("dxRootLimit = %d, want 508", dxRootLimit)
	}
	mk := func(n int) []hashedEntry {
		hes := make([]hashedEntry, n)
		for i := range hes {
			hes[i] = he(uint32(i*2+2), 1, fmt.Sprintf("n%d", i)) // distinct even majors
		}
		return hes
	}
	for _, n := range []int{dxRootLimit, dxRootLimit + 1} {
		leaves, err := packHtreeLeaves(mk(n), 16) // capacity 16 -> one 12-byte dirent per leaf
		if err != nil {
			t.Fatalf("packHtreeLeaves(%d): %v", n, err)
		}
		if len(leaves) != n {
			t.Fatalf("packHtreeLeaves(%d) produced %d leaves, want %d", n, len(leaves), n)
		}
	}
}

// TestEmitHtreeSentinelNoSideEffects proves the compute-then-commit contract: an
// infeasible (>508-leaf) emit returns the sentinel and leaves the directory's
// block 0, inode, and allocation state byte-for-byte unchanged. The synthetic
// entries reference a dummy inode, which is safe because emit rejects during the
// compute phase before it ever dereferences them.
func TestEmitHtreeSentinelNoSideEffects(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(16))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b := img.builder

	dInode, err := b.createDirectory(RootInode, "d", 0755, 0, 0)
	if err != nil {
		t.Fatalf("createDirectory: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := b.createFile(dInode, fmt.Sprintf("real%d", i), []byte("x"), 0644, 0, 0); err != nil {
			t.Fatalf("createFile: %v", err)
		}
	}

	before := captureDirSnapshot(t, img, dInode)

	err = b.emitHtree(dInode, RootInode, infeasibleEntries(8000), b.hashSeed, b.defHashVersion, b.signedHash)
	if !errors.Is(err, errHtreeDepth1Exceeded) {
		t.Fatalf("emitHtree = %v, want errHtreeDepth1Exceeded", err)
	}

	before.assertUnchanged(t, captureDirSnapshot(t, img, dInode))
}

// readLeafNames returns the real entry names in a directory leaf block.
func (b *builder) readLeafNames(t *testing.T, physBlock uint32) []string {
	t.Helper()
	block := make([]byte, blockSize)
	if err := b.disk.readAt(block, int64(b.layout.BlockOffset(physBlock))); err != nil {
		t.Fatalf("read leaf: %v", err)
	}
	var names []string
	for off := 0; off < blockSize; {
		recLen := int(binary.LittleEndian.Uint16(block[off+4:]))
		if recLen == 0 {
			break
		}
		if binary.LittleEndian.Uint32(block[off:]) != 0 {
			nameLen := int(block[off+6])
			names = append(names, string(block[off+8:off+8+nameLen]))
		}
		off += recLen
	}
	return names
}

// routedLeaf is one parsed dx_root leaf: its boundary hash, the names physically
// in it, and the minimum entry hash among them.
type routedLeaf struct {
	boundary uint32
	names    map[string]bool
	min      uint32
}

// parseOwnHtree reads an emitted directory's dx_root and leaves into a routing
// table, asserting the structural invariants (indexed, limit=508, depth-1,
// boundaries strictly ascending).
func parseOwnHtree(t *testing.T, b *builder, dInode uint32, effVer uint8) []routedLeaf {
	t.Helper()
	ino, err := b.readInode(dInode)
	if err != nil {
		t.Fatalf("readInode: %v", err)
	}
	if ino.Flags&inodeFlagIndex == 0 {
		t.Fatal("directory was not indexed")
	}
	blocks, err := b.getInodeBlocks(ino)
	if err != nil {
		t.Fatalf("getInodeBlocks: %v", err)
	}
	root := make([]byte, blockSize)
	if err := b.disk.readAt(root, int64(b.layout.BlockOffset(blocks[0]))); err != nil {
		t.Fatalf("read dx_root: %v", err)
	}

	limit := binary.LittleEndian.Uint16(root[dxCountLimitOffset:])
	count := int(binary.LittleEndian.Uint16(root[dxCountLimitOffset+2:]))
	if limit != dxRootLimit || root[dxRootInfoOffset+6] != 0 {
		t.Fatalf("dx_root limit=%d indirect=%d, want %d / 0", limit, root[dxRootInfoOffset+6], dxRootLimit)
	}
	if count != len(blocks)-1 {
		t.Fatalf("dx_root count=%d, want %d leaves", count, len(blocks)-1)
	}

	leaves := make([]routedLeaf, count)
	for i := 0; i < count; i++ {
		boundary, logical := uint32(0), binary.LittleEndian.Uint32(root[dxCountLimitOffset+4:])
		if i >= 1 {
			off := dxEntryArrayOffset + (i-1)*8
			boundary = binary.LittleEndian.Uint32(root[off:])
			logical = binary.LittleEndian.Uint32(root[off+4:])
		}
		set, min := make(map[string]bool), uint32(0xFFFFFFFF)
		for _, name := range b.readLeafNames(t, blocks[logical]) {
			set[name] = true
			if h, _ := ext4Dirhash([]byte(name), b.hashSeed, effVer); h < min {
				min = h
			}
		}
		leaves[i] = routedLeaf{boundary: boundary, names: set, min: min}
		if i >= 1 && boundary <= leaves[i-1].boundary {
			t.Fatalf("boundary not strictly ascending at %d: 0x%08x <= 0x%08x", i, boundary, leaves[i-1].boundary)
		}
	}
	return leaves
}

// TestEmitHtreeRoutingUnit validates OUR OWN emitted dx_root and hash routing
// entirely in-process (no Docker): every name must hash into the exact leaf the
// index routes it to, and each leaf's boundary must equal its minimum entry hash.
// This is the in-memory analog of the kernel open()-by-name oracle, so the
// own-emit path is no longer validated only under Docker.
func TestEmitHtreeRoutingUnit(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(64))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b := img.builder

	dInode, err := b.createDirectory(RootInode, "d", 0755, 0, 0)
	if err != nil {
		t.Fatalf("createDirectory: %v", err)
	}
	var names []string
	for i := 0; i < 2000; i++ {
		names = append(names, fmt.Sprintf("f%05d", i))
	}
	names = append(names, "café", "naïve", "Москва", "日本語")
	for _, n := range names {
		if _, err := b.createFile(dInode, n, []byte("x"), 0644, 0, 0); err != nil {
			t.Fatalf("createFile: %v", err)
		}
	}
	if err := b.emitHtreeDirs(); err != nil {
		t.Fatalf("emitHtreeDirs: %v", err)
	}

	effVer := effectiveHashVersion(b.defHashVersion, !b.signedHash)
	leaves := parseOwnHtree(t, b, dInode, effVer)

	// Every name must physically live in the leaf its hash routes to.
	for _, name := range names {
		h, _ := ext4Dirhash([]byte(name), b.hashSeed, effVer)
		idx := 0
		for i, lf := range leaves {
			if h >= lf.boundary {
				idx = i
			}
		}
		if !leaves[idx].names[name] {
			t.Errorf("name %q (hash 0x%08x) not in routed leaf %d [boundary 0x%08x]", name, h, idx, leaves[idx].boundary)
		}
	}

	// Each leaf's dx_entry boundary (i>=1) must equal its minimum entry hash.
	for i := 1; i < len(leaves); i++ {
		if leaves[i].boundary != leaves[i].min {
			t.Errorf("leaf %d boundary 0x%08x != min entry hash 0x%08x", i, leaves[i].boundary, leaves[i].min)
		}
	}
}

// TestAddDirEntryRefusesDepth2 verifies the mutation guard refuses an htree
// directory whose dx_root_info.indirect_levels is >= 1 (depth-2). It builds a
// real depth-1 htree, crafts the depth marker, then asserts an insert is rejected
// with an explicit error rather than mis-flattening a structure we cannot read.
func TestAddDirEntryRefusesDepth2(t *testing.T) {
	img, err := New(WithMemoryBackend(), WithSizeInMB(16))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b := img.builder

	dInode, err := b.createDirectory(RootInode, "d", 0755, 0, 0)
	if err != nil {
		t.Fatalf("createDirectory: %v", err)
	}
	for i := 0; i < 300; i++ { // ~4800 entry bytes > one leaf -> indexed
		if _, err := b.createFile(dInode, fmt.Sprintf("f%05d", i), []byte("x"), 0644, 0, 0); err != nil {
			t.Fatalf("createFile: %v", err)
		}
	}
	if err := b.emitHtreeDirs(); err != nil {
		t.Fatalf("emitHtreeDirs: %v", err)
	}

	ino, err := b.readInode(dInode)
	if err != nil {
		t.Fatalf("readInode: %v", err)
	}
	if ino.Flags&inodeFlagIndex == 0 {
		t.Fatalf("directory was not indexed; cannot test depth-2 guard")
	}

	// Craft indirect_levels = 1 (depth-2) in the dx_root.
	blocks, err := b.getInodeBlocks(ino)
	if err != nil {
		t.Fatalf("getInodeBlocks: %v", err)
	}
	block := make([]byte, blockSize)
	if err := b.disk.readAt(block, int64(b.layout.BlockOffset(blocks[0]))); err != nil {
		t.Fatalf("read dx_root: %v", err)
	}
	block[dxRootInfoOffset+6] = 1
	if err := b.disk.writeAt(block, int64(b.layout.BlockOffset(blocks[0]))); err != nil {
		t.Fatalf("write dx_root: %v", err)
	}

	err = b.addDirEntry(dInode, dirEntry{Inode: 11, Type: ftRegFile, Name: []byte("newentry")})
	if err == nil || !strings.Contains(err.Error(), "depth-2") {
		t.Fatalf("addDirEntry on depth-2 directory = %v, want a depth-2 refusal", err)
	}
}
