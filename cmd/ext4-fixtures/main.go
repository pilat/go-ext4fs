package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	ext4fs "github.com/pilat/go-ext4fs"
)

const (
	fixtureSizeMB     = 64
	fixturesCreatedAt = uint32(1600000000)
	expectedSHA256Hex = "b151e41a072581c31ddfc09992c272b6fd4e631aac3163eec7814212e82678a7"
)

func main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	_ = fs.Parse(os.Args[2:])

	switch cmd {
	case "generate":
		if err := runGenerate(); err != nil {
			log.Fatalf("generate failed: %v", err)
		}
	case "check":
		if err := runCheck(); err != nil {
			log.Fatalf("check failed: %v", err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	prog := filepath.Base(os.Args[0])
	fmt.Fprintf(os.Stderr, "usage: %s [generate|check]\n", prog)
}

func runGenerate() error {
	imagePath := defaultImagePath()

	size, fileHash, err := buildAndHashFixture(imagePath)
	if err != nil {
		return err
	}

	fingerprint := fixtureFingerprint(size, fileHash)

	fmt.Printf("fixture size: %d bytes\n", size)
	fmt.Printf("fixture file sha256: %s\n", fileHash)
	fmt.Printf("fixture fingerprint (sha256 of \"size:filehash\"): %s\n", fingerprint)
	fmt.Println()
	fmt.Println("update expected constant in cmd/ext4-fixtures/main.go if this value changed:")
	fmt.Printf("  const expectedSHA256Hex = %q\n", fingerprint)

	return nil
}

func runCheck() error {
	imagePath := defaultImagePath()

	size, fileHash, err := buildAndHashFixture(imagePath)
	if err != nil {
		return err
	}

	actual := fixtureFingerprint(size, fileHash)
	if actual != expectedSHA256Hex {
		log.Printf("ERROR: fingerprint mismatch: expected=%s actual=%s", expectedSHA256Hex, actual)
		return fmt.Errorf("fixture does not match expected fingerprint")
	}

	log.Printf("ok: fixture matches expected fingerprint (%s)", expectedSHA256Hex)
	return nil
}

func defaultImagePath() string {
	return filepath.Join(os.TempDir(), "ext4-full-features.img")
}

func buildAndHashFixture(imagePath string) (uint64, string, error) {
	// Ensure no stale file
	_ = os.Remove(imagePath)

	builder, err := ext4fs.NewWithCreatedAt(imagePath, fixtureSizeMB, fixturesCreatedAt)
	if err != nil {
		return 0, "", fmt.Errorf("failed to create image: %w", err)
	}
	defer func() {
		_ = builder.Close()
		_ = os.Remove(imagePath)
	}()

	if err := builder.PrepareFilesystem(); err != nil {
		return 0, "", fmt.Errorf("PrepareFilesystem failed: %w", err)
	}

	if err := buildFullFeaturesFixture(builder); err != nil {
		return 0, "", fmt.Errorf("fixture build failed: %w", err)
	}

	if err := builder.Save(); err != nil {
		return 0, "", fmt.Errorf("Save failed: %w", err)
	}

	info, err := os.Stat(imagePath)
	if err != nil {
		return 0, "", fmt.Errorf("failed to stat image %q: %w", imagePath, err)
	}

	f, err := os.Open(imagePath)
	if err != nil {
		return 0, "", fmt.Errorf("failed to open image %q: %w", imagePath, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return 0, "", fmt.Errorf("failed to hash image %q: %w", imagePath, err)
	}

	sum := h.Sum(nil)
	return uint64(info.Size()), hex.EncodeToString(sum), nil
}

func buildFullFeaturesFixture(b *ext4fs.Ext4ImageBuilder) error {
	root := uint32(ext4fs.RootInode)

	etcInode, err := b.CreateDirectory(root, "etc", 0o755, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to create /etc: %w", err)
	}

	_, err = b.CreateFile(etcInode, "hostname", []byte("ext4-fixture\n"), 0o644, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to create /etc/hostname: %w", err)
	}

	homeInode, err := b.CreateDirectory(root, "home", 0o755, 0, 0)
	if err != nil {
		return fmt.Errorf("failed to create /home: %w", err)
	}

	userInode, err := b.CreateDirectory(homeInode, "user", 0o700, 1000, 1000)
	if err != nil {
		return fmt.Errorf("failed to create /home/user: %w", err)
	}

	_, err = b.CreateFile(userInode, "note.txt", []byte("hello from ext4 fixtures\n"), 0o600, 1000, 1000)
	if err != nil {
		return fmt.Errorf("failed to create /home/user/note.txt: %w", err)
	}

	if err := b.SetXattr(userInode, "user.comment", []byte("example user directory")); err != nil {
		return fmt.Errorf("failed to set xattr on /home/user: %w", err)
	}

	return nil
}

func fixtureFingerprint(size uint64, fileHash string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d:%s", size, fileHash)
	return hex.EncodeToString(h.Sum(nil))
}
