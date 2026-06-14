package ext4fs_test

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilat/go-ext4fs"
)

// =============================================================================
// Foreign (mke2fs) htree image scaffold
// =============================================================================
//
// These helpers build a real ext4 htree directory with mke2fs + the Linux kernel
// inside the shared privileged container, then hand the raw image back to Go.
// The mkfs recipe pins single-group geometry that matches the library constants
// (4096-byte blocks, 256-byte inodes, 8192 inodes/group, 32768 blocks/group) and
// disables every feature the library cannot open (metadata_csum, resize_inode,
// 64bit, flex_bg, has_journal) so loadLayoutFromDisk accepts the image.
//
// Reused by the Task-2 hash de-risk test and the Task-6 foreign-maintenance tests.

// foreignMkfsOpts is the feature set that keeps a foreign image inside the
// library's open envelope (csum-off; no resize_inode reserved GDT blocks).
const foreignMkfsOpts = "dir_index,^metadata_csum,^resize_inode,^64bit,^flex_bg,^has_journal"

// asciiName is the deterministic ASCII filename for index i.
func asciiName(i int) string { return fmt.Sprintf("f%05d", i) }

// foreignHtreeImage builds a foreign htree directory at dirPath (relative to the
// filesystem root, e.g. "d" or "sub/d") containing asciiCount names (f00000..)
// plus extraNames (which may contain bytes >= 0x80), and returns the host image
// path together with the full ordered list of created names. Any parent
// components of dirPath are created as ordinary directories.
func foreignHtreeImage(t *testing.T, sizeMB, asciiCount int, extraNames []string, dirPath string) (imagePath string, names []string) {
	t.Helper()

	if !dockerAvailable || dockerContainerID == "" {
		t.Skip("Docker test container not available")
	}

	base := fmt.Sprintf("foreign-htree-%d.img", time.Now().UnixNano())
	imagePath = filepath.Join(sharedHostDir, base)
	remoteImage := filepath.Join(sharedContainerDir, base)

	// Write the high-byte names to a host file (exact bytes, no shell escaping).
	extraBase := fmt.Sprintf("foreign-names-%d.txt", time.Now().UnixNano())
	extraHostPath := filepath.Join(sharedHostDir, extraBase)
	remoteExtra := filepath.Join(sharedContainerDir, extraBase)
	if len(extraNames) > 0 {
		if err := os.WriteFile(extraHostPath, []byte(strings.Join(extraNames, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("write extra names file: %v", err)
		}
		t.Cleanup(func() { _ = os.Remove(extraHostPath) })
	}

	mountDir := fmt.Sprintf("/mnt/mkfs-%d", time.Now().UnixNano())
	script := fmt.Sprintf(`
set -e
rm -f %[1]s
dd if=/dev/zero of=%[1]s bs=1M count=%[2]d status=none
mkfs.ext4 -q -F -O %[3]s -b 4096 -I 256 -N 8192 %[1]s
mkdir -p %[4]s
mount -o loop %[1]s %[4]s
mkdir -p "%[4]s/%[8]s"
i=0
while [ $i -lt %[5]d ]; do printf 'f%%05d\n' $i; i=$((i+1)); done > /tmp/ascii_names_%[6]s
while IFS= read -r n; do : > "%[4]s/%[8]s/$n"; done < /tmp/ascii_names_%[6]s
if [ -f %[7]s ]; then while IFS= read -r n; do : > "%[4]s/%[8]s/$n"; done < %[7]s; fi
sync
umount %[4]s
chmod 0666 %[1]s
`, remoteImage, sizeMB, foreignMkfsOpts, mountDir, asciiCount, strconv.FormatInt(time.Now().UnixNano(), 10), remoteExtra, dirPath)

	stdout, stderr, err := dockerExecPrivileged(t, script)
	if err != nil {
		t.Fatalf("mke2fs scaffold failed: %v\nstdout: %s\nstderr: %s", err, stdout, stderr)
	}

	t.Cleanup(func() { _ = os.Remove(imagePath) })

	names = make([]string, 0, asciiCount+len(extraNames))
	for i := 0; i < asciiCount; i++ {
		names = append(names, asciiName(i))
	}
	names = append(names, extraNames...)
	return imagePath, names
}

// dockerExecPrivileged runs a shell script in the shared privileged container.
func dockerExecPrivileged(t *testing.T, script string) (stdout, stderr string, err error) {
	e := &testEnv{t: t}
	return e.runInContainer(script)
}

var reDebugfsInode = regexp.MustCompile(`Inode:\s+(\d+)`)

// debugfsInodeOf returns the inode number debugfs reports for a path.
func debugfsInodeOf(t *testing.T, imagePath, path string) uint32 {
	t.Helper()
	remoteImage := filepath.Join(sharedContainerDir, filepath.Base(imagePath))
	out, stderr, err := dockerExecPrivileged(t, fmt.Sprintf(`debugfs -R "stat %s" %s 2>/dev/null`, path, remoteImage))
	if err != nil {
		t.Fatalf("debugfs stat %s failed: %v\nstderr: %s", path, err, stderr)
	}
	m := reDebugfsInode.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("could not parse inode from debugfs stat %s:\n%s", path, out)
	}
	n, _ := strconv.Atoi(m[1])
	return uint32(n)
}

var reDebugfsFlags = regexp.MustCompile(`Flags:\s+0x([0-9a-fA-F]+)`)

// inodeFlagsByPath returns the i_flags debugfs reports for a path in the image.
func (e *testEnv) inodeFlagsByPath(t *testing.T, path string) uint32 {
	t.Helper()
	remoteImage := filepath.Join(sharedContainerDir, filepath.Base(e.imagePath))
	out, stderr, err := e.runInContainer(fmt.Sprintf(`debugfs -R "stat %s" %s 2>/dev/null`, path, remoteImage))
	if err != nil {
		t.Fatalf("debugfs stat %s failed: %v\nstderr: %s", path, err, stderr)
	}
	m := reDebugfsFlags.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("could not parse Flags from debugfs stat %s:\n%s", path, out)
	}
	v, _ := strconv.ParseUint(m[1], 16, 32)
	return uint32(v)
}

// debugfsHtreeDump returns `debugfs -R "htree_dump <dir>"` for the image.
func debugfsHtreeDump(t *testing.T, imagePath, dir string) string {
	t.Helper()
	remoteImage := filepath.Join(sharedContainerDir, filepath.Base(imagePath))
	out, stderr, err := dockerExecPrivileged(t, fmt.Sprintf(`debugfs -R "htree_dump %s" %s 2>/dev/null`, dir, remoteImage))
	if err != nil {
		t.Fatalf("htree_dump failed: %v\nstderr: %s", err, stderr)
	}
	return out
}

// readSuperblockHashParams reads s_hash_seed (0xEC), s_def_hash_version (0xFC)
// and the signedness from s_flags (0x160) directly from the image file.
func readSuperblockHashParams(t *testing.T, imagePath string) (seed [4]uint32, defHashVersion uint8, unsigned bool) {
	t.Helper()
	f, err := os.Open(imagePath)
	if err != nil {
		t.Fatalf("open image: %v", err)
	}
	defer func() { _ = f.Close() }()

	sb := make([]byte, 1024)
	if _, err := f.ReadAt(sb, 1024); err != nil {
		t.Fatalf("read superblock: %v", err)
	}
	for i := 0; i < 4; i++ {
		seed[i] = binary.LittleEndian.Uint32(sb[0xEC+i*4:])
	}
	defHashVersion = sb[0xFC]
	flags := binary.LittleEndian.Uint32(sb[0x160:])
	unsigned = flags&ext4fs.FlagsUnsignedHashForTest != 0
	return seed, defHashVersion, unsigned
}

// =============================================================================
// htree_dump parser
// =============================================================================

type htreeEntry struct {
	major, minor uint32
	name         string
}

type htreeLeaf struct {
	boundary uint32 // dx_entry hash that routes to this leaf (0 for leaf 0)
	entries  []htreeEntry
}

type parsedHtree struct {
	hashVersion    int
	indirectLevels int
	count, limit   int
	rootHashes     []uint32 // boundary hashes in dx_root order (ascending, incl 0)
	leaves         []htreeLeaf
}

var (
	reInfoInt    = regexp.MustCompile(`(?m)^\s*(Hash Version|Indirect levels):\s+(\d+)`)
	reCountLimit = regexp.MustCompile(`(?m)^Number of entries \((count|limit)\):\s+(\d+)`)
	reRootEntry  = regexp.MustCompile(`(?m)^Entry #\d+: Hash (0x[0-9a-fA-F]+), block \d+\s*$`)
	reLeafHeader = regexp.MustCompile(`Entry #\d+: Hash (0x[0-9a-fA-F]+), block \d+\nReading directory block`)
	reLeafEntry  = regexp.MustCompile(`\d+\s+0x([0-9a-fA-F]+)-([0-9a-fA-F]+)\s+\(\d+\)\s+(\S+)`)
)

func parseHtreeDump(t *testing.T, dump string) parsedHtree {
	t.Helper()
	var p parsedHtree

	for _, m := range reInfoInt.FindAllStringSubmatch(dump, -1) {
		n, _ := strconv.Atoi(m[2])
		switch m[1] {
		case "Hash Version":
			p.hashVersion = n
		case "Indirect levels":
			p.indirectLevels = n
		}
	}
	for _, m := range reCountLimit.FindAllStringSubmatch(dump, -1) {
		n, _ := strconv.Atoi(m[2])
		switch m[1] {
		case "count":
			p.count = n
		case "limit":
			p.limit = n
		}
	}

	// Root index: the contiguous run of "Entry #..., block ..." lines that appear
	// before the first leaf dump (i.e. not followed by "Reading directory block").
	rootSection := dump
	if hdr := reLeafHeader.FindStringIndex(dump); hdr != nil {
		// The root index lines are those before the first leaf header.
		rootSection = dump[:hdr[0]]
	}
	for _, m := range reRootEntry.FindAllStringSubmatch(rootSection, -1) {
		v, _ := strconv.ParseUint(m[1][2:], 16, 32)
		p.rootHashes = append(p.rootHashes, uint32(v))
	}

	// Leaf sections: split on each leaf header.
	headers := reLeafHeader.FindAllStringSubmatchIndex(dump, -1)
	for i, h := range headers {
		boundaryHex := dump[h[2]:h[3]]
		v, _ := strconv.ParseUint(boundaryHex[2:], 16, 32)

		end := len(dump)
		if i+1 < len(headers) {
			end = headers[i+1][0]
		}
		section := dump[h[1]:end]

		leaf := htreeLeaf{boundary: uint32(v)}
		for _, e := range reLeafEntry.FindAllStringSubmatch(section, -1) {
			maj, _ := strconv.ParseUint(e[1], 16, 32)
			min, _ := strconv.ParseUint(e[2], 16, 32)
			leaf.entries = append(leaf.entries, htreeEntry{major: uint32(maj), minor: uint32(min), name: e[3]})
		}
		p.leaves = append(p.leaves, leaf)
	}

	return p
}

// =============================================================================
// Task 2 — on-disk de-risk: our hash reproduces a real mke2fs htree
// =============================================================================

// TestDirhashReproducesForeignHtree is the Task-2 acceptance gate. It builds a
// real mke2fs htree directory (with names that include bytes >= 0x80), then
// proves ext4Dirhash, fed the image's own seed and signedness, reproduces every
// stored dx_entry boundary hash and routes every entry into the leaf the kernel
// actually placed it in. A wrong hash would leave readdir working but make
// open()-by-name silently miss, so this must hold byte-for-byte.
func TestDirhashReproducesForeignHtree(t *testing.T) {
	skipIfNoDocker(t)

	const asciiCount = 1200
	// Names with bytes >= 0x80 are required: the signed and unsigned half_md4
	// hashes are bit-identical for pure ASCII, so only high-byte names exercise
	// the signedness path on-disk.
	extra := []string{"café", "äüö", "naïve", "Москва", "日本語"}

	imagePath, created := foreignHtreeImage(t, 64, asciiCount, extra, "d")
	seed, defHashVersion, unsigned := readSuperblockHashParams(t, imagePath)
	effVer := ext4fs.EffectiveHashVersionForTest(defHashVersion, unsigned)

	dump := debugfsHtreeDump(t, imagePath, "/d")
	p := parseHtreeDump(t, dump)

	t.Logf("hashVersion=%d unsigned=%v effVer=%d count=%d limit=%d leaves=%d seed=%08x",
		p.hashVersion, unsigned, effVer, p.count, p.limit, len(p.leaves), seed[0])

	// Structural sanity: a depth-1 htree with the no-csum dx_root limit.
	if p.indirectLevels != 0 {
		t.Fatalf("expected depth-1 htree (indirect levels 0), got %d", p.indirectLevels)
	}
	if p.limit != 508 {
		t.Errorf("dx_root limit = %d, want 508 (no metadata_csum)", p.limit)
	}
	if p.count < 2 || len(p.leaves) < 2 {
		t.Fatalf("expected a multi-leaf htree, got count=%d leaves=%d", p.count, len(p.leaves))
	}
	if int(defHashVersion) != p.hashVersion {
		t.Errorf("SB def_hash_version %d disagrees with dx_root hash version %d", defHashVersion, p.hashVersion)
	}

	hashOf := func(name string) uint32 {
		h, _ := ext4fs.Ext4DirhashForTest([]byte(name), seed, effVer)
		return h
	}
	verifyForeignBoundaries(t, p, created, hashOf)
	if seen := verifyForeignLeaves(t, p, created, hashOf); seen != len(created) {
		t.Errorf("htree_dump enumerated %d entries, but %d names were created", seen, len(created))
	}
}

// verifyForeignBoundaries asserts the dx_root boundary hashes are strictly
// ascending and each (after slot 0) is reproduced by ext4Dirhash of some name.
func verifyForeignBoundaries(t *testing.T, p parsedHtree, created []string, hashOf func(string) uint32) {
	t.Helper()
	computed := make(map[uint32]bool, len(created))
	for _, n := range created {
		computed[hashOf(n)] = true
	}
	for i := 1; i < len(p.rootHashes); i++ {
		if p.rootHashes[i] <= p.rootHashes[i-1] {
			t.Errorf("root boundary %d (0x%08x) not strictly greater than previous (0x%08x)", i, p.rootHashes[i], p.rootHashes[i-1])
		}
		if !computed[p.rootHashes[i]] {
			t.Errorf("stored dx_entry boundary 0x%08x is not reproduced by ext4Dirhash of any name", p.rootHashes[i])
		}
	}
}

// verifyForeignLeaves reproduces each entry's on-disk major hash byte-exactly,
// confirms it routes into its leaf's [boundary_i, boundary_{i+1}) range, and
// checks each leaf's boundary equals its minimum entry hash. Returns the total
// number of entries enumerated.
func verifyForeignLeaves(t *testing.T, p parsedHtree, created []string, hashOf func(string) uint32) int {
	t.Helper()
	createdSet := make(map[string]bool, len(created))
	for _, n := range created {
		createdSet[n] = true
	}

	var seen int
	for li, leaf := range p.leaves {
		nextBoundary := uint32(0xFFFFFFFF)
		hasNext := li+1 < len(p.leaves)
		if hasNext {
			nextBoundary = p.leaves[li+1].boundary
		}
		leafMin := uint32(0xFFFFFFFF)
		for _, e := range leaf.entries {
			seen++
			if !createdSet[e.name] {
				t.Errorf("leaf %d entry %q was not one of the created names", li, e.name)
				continue
			}
			got := hashOf(e.name)
			if got != e.major {
				t.Errorf("hash mismatch for %q: ext4Dirhash=0x%08x, on-disk=0x%08x", e.name, got, e.major)
			}
			if got < leaf.boundary || (hasNext && got >= nextBoundary) {
				t.Errorf("entry %q hash 0x%08x outside leaf %d range [0x%08x, 0x%08x)", e.name, got, li, leaf.boundary, nextBoundary)
			}
			if got < leafMin {
				leafMin = got
			}
		}
		if li >= 1 && len(leaf.entries) > 0 && leaf.boundary != leafMin {
			t.Errorf("leaf %d boundary 0x%08x != min entry hash 0x%08x", li, leaf.boundary, leafMin)
		}
	}
	return seen
}

// =============================================================================
// Task 3 — htree-aware enumeration on a foreign image
// =============================================================================

// sameNameSet reports whether got and want contain the same names (ignoring
// order), comparing as multisets so a duplicated name is caught, not collapsed.
func sameNameSet(t *testing.T, got, want []string, ctx string) {
	t.Helper()
	gs := make(map[string]int, len(got))
	for _, n := range got {
		gs[n]++
	}
	ws := make(map[string]int, len(want))
	for _, n := range want {
		ws[n]++
	}
	if len(got) != len(want) {
		t.Errorf("%s: got %d names, want %d", ctx, len(got), len(want))
	}
	var diff []string
	for n, wc := range ws {
		if gs[n] != wc {
			diff = append(diff, fmt.Sprintf("%q (got %d, want %d)", n, gs[n], wc))
		}
	}
	for n, gc := range gs {
		if _, ok := ws[n]; !ok {
			diff = append(diff, fmt.Sprintf("%q (got %d, want 0)", n, gc))
		}
	}
	if len(diff) > 0 {
		if len(diff) > 10 {
			diff = diff[:10]
		}
		t.Errorf("%s: name multiset mismatch (sample: %v)", ctx, diff)
	}
}

// TestForeignHtreeEnumeration is the Task-3 acceptance gate. It opens a real
// mke2fs htree directory and confirms our existing readers — listDirEntries and
// the new readAllEntries — enumerate the complete entry set, and that
// readAllEntries recovers the directory's true parent inode (cross-checked
// against debugfs, which is the only oracle that exposes the ".." target value).
// The directory is nested (sub/d) so the parent inode is a real allocated value,
// not the well-known root inode.
func TestForeignHtreeEnumeration(t *testing.T) {
	skipIfNoDocker(t)

	const asciiCount = 1000
	extra := []string{"café", "naïve", "Москва"}
	imagePath, created := foreignHtreeImage(t, 64, asciiCount, extra, "sub/d")

	img, err := ext4fs.Open(ext4fs.WithExistingImagePath(imagePath))
	if err != nil {
		t.Fatalf("open foreign image: %v", err)
	}
	defer func() { _ = img.Close() }()

	subInode, err := img.FindEntryForTest(ext4fs.RootInode, "sub")
	if err != nil || subInode == 0 {
		t.Fatalf("findEntry(root, sub) = %d, %v", subInode, err)
	}
	dInode, err := img.FindEntryForTest(subInode, "d")
	if err != nil || dInode == 0 {
		t.Fatalf("findEntry(sub, d) = %d, %v", dInode, err)
	}

	// The directory must actually be htree, or this test proves nothing.
	flags, err := img.InodeFlagsForTest(dInode)
	if err != nil {
		t.Fatalf("read flags: %v", err)
	}
	if flags&ext4fs.InodeFlagIndexForTest == 0 {
		t.Fatalf("directory inode %d is not htree-indexed (flags=0x%x); test would be vacuous", dInode, flags)
	}

	// listDirEntries must enumerate the complete set on an htree directory.
	listed, err := img.ListNamesForTest(dInode)
	if err != nil {
		t.Fatalf("listDirEntries: %v", err)
	}
	sameNameSet(t, listed, created, "listDirEntries")

	// readAllEntries must enumerate the same set AND recover the parent inode.
	all, parentInode, err := img.ReadAllEntriesForTest(dInode)
	if err != nil {
		t.Fatalf("readAllEntries: %v", err)
	}
	sameNameSet(t, all, created, "readAllEntries")

	wantParent := debugfsInodeOf(t, imagePath, "/sub")
	if parentInode != wantParent {
		t.Errorf("recovered parent inode = %d, want %d (debugfs /sub)", parentInode, wantParent)
	}
	if parentInode != subInode {
		t.Errorf("recovered parent inode = %d, but findEntry located sub at %d", parentInode, subInode)
	}

	// Independent count via the mounted kernel ("compare to in-container ls"),
	// which also runs e2fsck on the foreign image. dockerExecSimple prepends the
	// e2fsck pass output, so take the last line (the wc -l result).
	env := &testEnv{t: t, imagePath: imagePath}
	out := env.dockerExecSimple("ls -1 sub/d | wc -l")
	if got := lastLine(out); got != strconv.Itoa(len(created)) {
		t.Errorf("ls -1 sub/d | wc -l = %q, want %d", got, len(created))
	}
}

// lastLine returns the last non-empty, trimmed line of s.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if t := strings.TrimSpace(lines[i]); t != "" {
			return t
		}
	}
	return ""
}
