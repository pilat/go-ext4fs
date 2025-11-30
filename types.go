package ext4fs

// ============================================================================
// Constants
// ============================================================================

const (
	// Disk geometry
	SectorSize        = 512
	BlockSize         = 4096
	BlocksPerGroup    = 32768
	InodesPerGroup    = 8192
	InodeSize         = 256
	FirstDataBlock    = 0 // For 4K blocks, superblock is in block 0
	ReservedGDTBlocks = 256

	// Magic numbers
	Ext4Magic = 0xEF53
	MBRMagic  = 0xAA55

	// Inode numbers
	BadInode         = 1
	RootInode        = 2
	UserQuotaInode   = 3
	GroupQuotaInode  = 4
	BootLoaderInode  = 5
	UndeleteDirInode = 6
	ResizeInode      = 7
	JournalInode     = 8
	FirstNonResInode = 11

	// File types for directory entries
	FTUnknown = 0
	FTRegFile = 1
	FTDir     = 2
	FTChrdev  = 3
	FTBlkdev  = 4
	FTFifo    = 5
	FTSock    = 6
	FTSymlink = 7

	// Inode mode flags
	S_IXOTH  = 0x0001
	S_IWOTH  = 0x0002
	S_IROTH  = 0x0004
	S_IXGRP  = 0x0008
	S_IWGRP  = 0x0010
	S_IRGRP  = 0x0020
	S_IXUSR  = 0x0040
	S_IWUSR  = 0x0080
	S_IRUSR  = 0x0100
	S_ISVTX  = 0x0200 // Sticky bit
	S_ISGID  = 0x0400
	S_ISUID  = 0x0800
	S_IFIFO  = 0x1000
	S_IFCHR  = 0x2000
	S_IFDIR  = 0x4000
	S_IFBLK  = 0x6000
	S_IFREG  = 0x8000
	S_IFLNK  = 0xA000
	S_IFSOCK = 0xC000

	// Feature flags
	CompatDirPrealloc  = 0x0001
	CompatHasJournal   = 0x0004
	CompatExtAttr      = 0x0008
	CompatResizeInode  = 0x0010
	CompatDirIndex     = 0x0020
	CompatSparseSuper2 = 0x0200

	IncompatFiletype = 0x0002
	IncompatExtents  = 0x0040
	Incompat64Bit    = 0x0080
	IncompatFlexBG   = 0x0200

	ROCompatSparseSuper  = 0x0001
	ROCompatLargeFile    = 0x0002
	ROCompatHugeFile     = 0x0008
	ROCompatGDTCsum      = 0x0010
	ROCompatDirNlink     = 0x0020
	ROCompatExtraIsize   = 0x0040
	ROCompatMetadataCsum = 0x0400
)

// ============================================================================
// On-disk structures
// ============================================================================

// MBRPartitionEntry represents a single MBR partition entry
type MBRPartitionEntry struct {
	Status      uint8
	StartCHS    [3]byte
	Type        uint8
	EndCHS      [3]byte
	StartLBA    uint32
	SectorCount uint32
}

// MBR represents the Master Boot Record
type MBR struct {
	BootCode   [446]byte
	Partitions [4]MBRPartitionEntry
	Signature  uint16
}

// Ext4Superblock represents the ext4 superblock structure
type Ext4Superblock struct {
	InodesCount       uint32 // Total inode count
	BlocksCountLo     uint32 // Total block count (low 32 bits)
	RBlocksCountLo    uint32 // Reserved block count (low 32 bits)
	FreeBlocksCountLo uint32 // Free block count (low 32 bits)
	FreeInodesCount   uint32 // Free inode count
	FirstDataBlock    uint32 // First data block
	LogBlockSize      uint32 // Block size = 2^(10 + log_block_size)
	LogClusterSize    uint32 // Cluster size
	BlocksPerGroup    uint32 // Blocks per group
	ClustersPerGroup  uint32 // Clusters per group
	InodesPerGroup    uint32 // Inodes per group
	MTime             uint32 // Mount time
	WTime             uint32 // Write time
	MntCount          uint16 // Mount count
	MaxMntCount       uint16 // Max mount count
	Magic             uint16 // Magic signature (0xEF53)
	State             uint16 // File system state
	Errors            uint16 // Error handling behavior
	MinorRevLevel     uint16 // Minor revision level
	LastCheck         uint32 // Last check time
	CheckInterval     uint32 // Check interval
	CreatorOS         uint32 // Creator OS
	RevLevel          uint32 // Revision level
	DefResUID         uint16 // Default UID for reserved blocks
	DefResGID         uint16 // Default GID for reserved blocks

	// EXT4_DYNAMIC_REV specific
	FirstIno          uint32   // First non-reserved inode
	InodeSize         uint16   // Inode size
	BlockGroupNr      uint16   // Block group number of this superblock
	FeatureCompat     uint32   // Compatible feature set
	FeatureIncompat   uint32   // Incompatible feature set
	FeatureROCompat   uint32   // Read-only compatible feature set
	UUID              [16]byte // 128-bit UUID
	VolumeName        [16]byte // Volume name
	LastMounted       [64]byte // Last mounted directory
	AlgorithmUsageBmp uint32   // Compression algorithm bitmap

	// Performance hints
	PreallocBlocks    uint8  // Blocks to preallocate for files
	PreallocDirBlocks uint8  // Blocks to preallocate for directories
	ReservedGDTBlocks uint16 // Reserved GDT blocks for online growth

	// Journaling support
	JournalUUID      [16]byte   // UUID of journal superblock
	JournalInum      uint32     // Inode number of journal file
	JournalDev       uint32     // Device number of journal file
	LastOrphan       uint32     // Head of orphan inode list
	HashSeed         [4]uint32  // HTREE hash seed
	DefHashVersion   uint8      // Default hash version
	JnlBackupType    uint8      // Journal backup type
	DescSize         uint16     // Group descriptor size
	DefaultMountOpts uint32     // Default mount options
	FirstMetaBg      uint32     // First metablock block group
	MkfsTime         uint32     // Filesystem creation time
	JnlBlocks        [17]uint32 // Backup of journal inodes

	// 64-bit support
	BlocksCountHi        uint32     // High 32-bits of block count
	RBlocksCountHi       uint32     // High 32-bits of reserved blocks
	FreeBlocksCountHi    uint32     // High 32-bits of free blocks
	MinExtraIsize        uint16     // All inodes have at least this much extra
	WantExtraIsize       uint16     // New inodes should reserve this much
	Flags                uint32     // Miscellaneous flags
	RaidStride           uint16     // RAID stride
	MmpInterval          uint16     // Seconds to wait in MMP checking
	MmpBlock             uint64     // Block for multi-mount protection
	RaidStripeWidth      uint32     // Blocks on all data disks (N * stride)
	LogGroupsPerFlex     uint8      // FLEX_BG group size
	ChecksumType         uint8      // Metadata checksum algorithm
	ReservedPad          uint16     // Padding
	KbytesWritten        uint64     // Kbytes written lifetime
	SnapshotInum         uint32     // Inode of active snapshot
	SnapshotID           uint32     // Sequential ID of active snapshot
	SnapshotRBlocksCount uint64     // Reserved blocks for snapshot
	SnapshotList         uint32     // Inode of snapshot list head
	ErrorCount           uint32     // Number of errors
	FirstErrorTime       uint32     // First time an error happened
	FirstErrorIno        uint32     // Inode involved in first error
	FirstErrorBlock      uint64     // Block involved in first error
	FirstErrorFunc       [32]byte   // Function where error happened
	FirstErrorLine       uint32     // Line number where error happened
	LastErrorTime        uint32     // Last time an error happened
	LastErrorIno         uint32     // Inode involved in last error
	LastErrorLine        uint32     // Line number where last error happened
	LastErrorBlock       uint64     // Block involved in last error
	LastErrorFunc        [32]byte   // Function where last error happened
	MountOpts            [64]byte   // Mount options string
	UsrQuotaInum         uint32     // Inode for user quota
	GrpQuotaInum         uint32     // Inode for group quota
	OverheadBlocks       uint32     // Overhead blocks
	BackupBgs            [2]uint32  // Groups with sparse_super2 superblocks
	EncryptAlgos         [4]uint8   // Encryption algorithms
	EncryptPwSalt        [16]byte   // Salt for string2key
	LpfIno               uint32     // Lost+found inode
	PrjQuotaInum         uint32     // Inode for project quota
	ChecksumSeed         uint32     // Checksum seed
	WTimeHi              uint8      // High 8 bits of write time
	MTimeHi              uint8      // High 8 bits of mount time
	MkfsTimeHi           uint8      // High 8 bits of mkfs time
	LastCheckHi          uint8      // High 8 bits of lastcheck
	FirstErrorTimeHi     uint8      // High 8 bits of first error time
	LastErrorTimeHi      uint8      // High 8 bits of last error time
	Pad                  [2]uint8   // Padding
	Encoding             uint16     // Filename charset encoding
	EncodingFlags        uint16     // Filename charset encoding flags
	Reserved             [95]uint32 // Padding to the end of the block
	Checksum             uint32     // Superblock checksum
}

// Ext4GroupDesc represents a block group descriptor (64-bit version)
type Ext4GroupDesc struct {
	BlockBitmapLo     uint32 // Block bitmap block (low 32 bits)
	InodeBitmapLo     uint32 // Inode bitmap block (low 32 bits)
	InodeTableLo      uint32 // Inode table block (low 32 bits)
	FreeBlocksCountLo uint16 // Free blocks count (low 16 bits)
	FreeInodesCountLo uint16 // Free inodes count (low 16 bits)
	UsedDirsCountLo   uint16 // Directories count (low 16 bits)
	Flags             uint16 // Block group flags
	ExcludeBitmapLo   uint32 // Exclude bitmap block (low 32 bits)
	BlockBitmapCsumLo uint16 // Block bitmap checksum (low 16 bits)
	InodeBitmapCsumLo uint16 // Inode bitmap checksum (low 16 bits)
	ItableUnusedLo    uint16 // Unused inodes count (low 16 bits)
	Checksum          uint16 // Group descriptor checksum
	// 64-bit fields
	BlockBitmapHi     uint32 // Block bitmap block (high 32 bits)
	InodeBitmapHi     uint32 // Inode bitmap block (high 32 bits)
	InodeTableHi      uint32 // Inode table block (high 32 bits)
	FreeBlocksCountHi uint16 // Free blocks count (high 16 bits)
	FreeInodesCountHi uint16 // Free inodes count (high 16 bits)
	UsedDirsCountHi   uint16 // Directories count (high 16 bits)
	ItableUnusedHi    uint16 // Unused inodes count (high 16 bits)
	ExcludeBitmapHi   uint32 // Exclude bitmap block (high 32 bits)
	BlockBitmapCsumHi uint16 // Block bitmap checksum (high 16 bits)
	InodeBitmapCsumHi uint16 // Inode bitmap checksum (high 16 bits)
	Reserved          uint32 // Padding
}

// Ext4GroupDesc32 is the 32-byte on-disk descriptor used when DescSize=32.
type Ext4GroupDesc32 struct {
	BlockBitmapLo     uint32
	InodeBitmapLo     uint32
	InodeTableLo      uint32
	FreeBlocksCountLo uint16
	FreeInodesCountLo uint16
	UsedDirsCountLo   uint16
	Flags             uint16
	ExcludeBitmapLo   uint32
	BlockBitmapCsumLo uint16
	InodeBitmapCsumLo uint16
	ItableUnusedLo    uint16
	Checksum          uint16
}

// Ext4Inode represents an inode structure
type Ext4Inode struct {
	Mode       uint16   // File mode
	UID        uint16   // Owner UID (low 16 bits)
	SizeLo     uint32   // File size (low 32 bits)
	ATime      uint32   // Access time
	CTime      uint32   // Inode change time
	MTime      uint32   // Modification time
	DTime      uint32   // Deletion time
	GID        uint16   // Group ID (low 16 bits)
	LinksCount uint16   // Hard links count
	BlocksLo   uint32   // Block count (low 32 bits)
	Flags      uint32   // File flags
	OSD1       uint32   // OS-dependent 1
	Block      [60]byte // Block pointers or extent tree
	Generation uint32   // File version (for NFS)
	FileACLLo  uint32   // Extended attributes block (low)
	SizeHi     uint32   // File size (high 32 bits) / Dir ACL
	ObsoFaddr  uint32   // Obsolete fragment address
	// OS-dependent 2
	BlocksHi   uint16 // Block count (high 16 bits)
	FileACLHi  uint16 // Extended attributes block (high)
	UIDHi      uint16 // Owner UID (high 16 bits)
	GIDHi      uint16 // Group ID (high 16 bits)
	ChecksumLo uint16 // Inode checksum (low 16 bits)
	Reserved   uint16 // Reserved
	// Extra fields for larger inodes
	ExtraIsize  uint16 // Extra inode size
	ChecksumHi  uint16 // Inode checksum (high 16 bits)
	CTimeExtra  uint32 // Extra ctime (nsec << 2 | epoch)
	MTimeExtra  uint32 // Extra mtime
	ATimeExtra  uint32 // Extra atime
	CrTime      uint32 // File creation time
	CrTimeExtra uint32 // Extra file creation time
	VersionHi   uint32 // Version (high 32 bits)
	Projid      uint32 // Project ID
}

// Ext4ExtentHeader represents extent tree header
type Ext4ExtentHeader struct {
	Magic      uint16 // Magic number (0xF30A)
	Entries    uint16 // Number of valid entries
	Max        uint16 // Max number of entries
	Depth      uint16 // Depth of tree (0 = leaf)
	Generation uint32 // Generation
}

// Ext4Extent represents a data extent
type Ext4Extent struct {
	Block   uint32 // First logical block
	Len     uint16 // Number of blocks
	StartHi uint16 // High 16 bits of physical block
	StartLo uint32 // Low 32 bits of physical block
}

// Ext4DirEntry2 represents a directory entry (version 2)
type Ext4DirEntry2 struct {
	Inode    uint32 // Inode number
	RecLen   uint16 // Directory entry length
	NameLen  uint8  // Name length
	FileType uint8  // File type
	Name     []byte // File name (up to 255 bytes)
}
