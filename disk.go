package ext4fs

import (
	"fmt"
	"os"
	"path/filepath"
)

// NewExt4ImageBuilder creates an ext4 image builder that writes directly to
// the given image file path. The file is created/truncated to the requested
// size (in megabytes) and all filesystem structures are written to it
// incrementally, avoiding large in-memory buffers.
func NewExt4ImageBuilder(imagePath string, totalSizeMB int) (*Ext4ImageBuilder, error) {
	if totalSizeMB <= 0 {
		return nil, fmt.Errorf("totalSizeMB must be > 0, got %d", totalSizeMB)
	}

	totalSize := uint64(totalSizeMB) * 1024 * 1024

	dir := filepath.Dir(imagePath)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating directory for image %q: %w", imagePath, err)
		}
	}

	f, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening image file %q: %w", imagePath, err)
	}

	if err := f.Truncate(int64(totalSize)); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("truncating image file %q to %d bytes: %w", imagePath, totalSize, err)
	}

	backend := &fileBackend{f: f}
	return newExt4ImageBuilder(backend, imagePath, totalSize), nil
}

// Save flushes any buffered data to the underlying disk image. The builder
// writes directly to the image file during construction, so Save primarily
// ensures the image is durable on disk.
func (b *Ext4ImageBuilder) Save() error {
	if b == nil {
		return fmt.Errorf("nil Ext4ImageBuilder")
	}

	// If the backend supports Sync, ensure data is persisted.
	if syncer, ok := b.disk.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return fmt.Errorf("failed to sync image %q: %w", b.imagePath, err)
		}
	}

	if DEBUG {
		fmt.Printf("✓ Image saved to: %s (%d MB)\n", b.imagePath, b.totalSize/(1024*1024))
	}
	return nil
}

// Close releases any resources associated with the underlying disk backend.
// It is safe to call Close multiple times.
func (b *Ext4ImageBuilder) Close() error {
	if b == nil || b.disk == nil {
		return nil
	}

	if closer, ok := b.disk.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("failed to close image %q: %w", b.imagePath, err)
		}
	}

	b.disk = nil
	return nil
}

// diskBackend abstracts the storage used for the ext4 image. The core
// builder logic is written against this interface so it can operate on
// any random-access block device (files, in-memory buffers, etc.).
type diskBackend interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
}

// fileBackend is a thin wrapper around *os.File implementing diskBackend.
type fileBackend struct {
	f *os.File
}

func (fb *fileBackend) ReadAt(p []byte, off int64) (int, error) {
	return fb.f.ReadAt(p, off)
}

func (fb *fileBackend) WriteAt(p []byte, off int64) (int, error) {
	return fb.f.WriteAt(p, off)
}

func (fb *fileBackend) Sync() error {
	return fb.f.Sync()
}

func (fb *fileBackend) Close() error {
	return fb.f.Close()
}
