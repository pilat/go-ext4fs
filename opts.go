package ext4fs

// ImageOption is a functional option for configuring Image creation.
type ImageOption func(*Image)

// WithImagePath sets the image path.
func WithImagePath(imagePath string) ImageOption {
	return func(i *Image) {
		i.imagePath = imagePath
	}
}

// WithSizeInMB sets the image size in MB.
func WithSizeInMB(sizeMB int) ImageOption {
	return func(i *Image) {
		i.sizeBytes = uint64(sizeMB) * 1024 * 1024
	}
}

// WithSize sets image size in bytes.
func WithSize(sizeBytes uint64) ImageOption {
	return func(i *Image) {
		i.sizeBytes = sizeBytes
	}
}

// WithCreatedAt sets the creation timestamp.
func WithCreatedAt(createdAt uint32) ImageOption {
	return func(i *Image) {
		i.createdAt = createdAt
	}
}
