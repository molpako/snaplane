package restorepopulator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	snaplanev1alpha1 "github.com/molpako/snaplane/api/v1alpha1"
)

// WorkerOptions defines required inputs for restore-worker mode.
type WorkerOptions struct {
	SourceDir       string
	ManifestID      string
	TargetDevice    string
	Format          string
	VolumeSizeBytes int64
	ChunkSizeBytes  int64
}

const (
	defaultCASMaxChainDepth = 1024
	envCASMaxChainDepth     = "SNAPLANE_CAS_MAX_CHAIN_DEPTH"

	casRepoVersionV1  = 1
	casIndexVersionV1 = 1
)

// RunWorker restores one backup object by copying source data into target block device.
func RunWorker(opts WorkerOptions) error {
	if opts.SourceDir == "" {
		return fmt.Errorf("source-dir is required")
	}
	if opts.ManifestID == "" {
		return fmt.Errorf("manifest-id is required")
	}
	if opts.TargetDevice == "" {
		return fmt.Errorf("target-device is required")
	}

	sourceDir := filepath.Clean(opts.SourceDir)
	if !filepath.IsAbs(sourceDir) {
		return fmt.Errorf("source-dir must be absolute: %q", opts.SourceDir)
	}

	sourcePath := filepath.Clean(filepath.Join(sourceDir, opts.ManifestID))
	rel, err := filepath.Rel(sourceDir, sourcePath)
	if err != nil {
		return fmt.Errorf("resolve source path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("manifest-id %q escapes source-dir", opts.ManifestID)
	}

	switch opts.Format {
	case "", snaplanev1alpha1.RestoreSourceFormatMockImageV1:
		return runMockWorker(sourcePath, opts.TargetDevice)
	case snaplanev1alpha1.RestoreSourceFormatCASV1:
		return runCASWorker(opts)
	default:
		return fmt.Errorf("unsupported restore format %q", opts.Format)
	}
}

func runMockWorker(sourcePath string, targetDevice string) error {
	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source data %q: %w", sourcePath, err)
	}
	defer src.Close()

	dst, err := os.OpenFile(filepath.Clean(targetDevice), os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open target device %q: %w", targetDevice, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy restore payload: %w", err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sync target device: %w", err)
	}
	return nil
}

type casManifest struct {
	ManifestID       string `json:"manifestID"`
	ParentManifestID string `json:"parentManifestID,omitempty"`
	Volume           struct {
		SizeBytes      int64 `json:"sizeBytes"`
		ChunkSizeBytes int64 `json:"chunkSizeBytes"`
	} `json:"volume"`
	Segments struct {
		Change string `json:"change"`
	} `json:"segments"`
}

type casRepoMeta struct {
	RepoVersion int    `json:"repoVersion"`
	ChunkHash   string `json:"chunkHash"`
	Compression string `json:"compression"`
}

type casChangeRecord struct {
	ChunkIndex uint64 `json:"chunkIndex"`
	ChunkID    string `json:"chunkID,omitempty"`
	State      string `json:"state"`
}

type casActiveIndex struct {
	Version  int      `json:"version"`
	Segments []string `json:"segments"`
}

type casIndexEntry struct {
	ChunkID      string `json:"chunkID"`
	PackID       int    `json:"packID"`
	PackOffset   int64  `json:"packOffset"`
	StoredLength int64  `json:"storedLength"`
	RawLength    int64  `json:"rawLength"`
	Codec        string `json:"codec"`
}

type casResolvedChunk struct {
	state   string
	chunkID string
}

func runCASWorker(opts WorkerOptions) error {
	if err := validateCASRepoMeta(filepath.Join(opts.SourceDir, "repo.json")); err != nil {
		return err
	}
	manifestPath := filepath.Join(opts.SourceDir, "snapshots", opts.ManifestID, "manifest.json")
	latest, err := loadCASManifest(manifestPath)
	if err != nil {
		return err
	}

	chain, err := resolveCASManifestChain(opts.SourceDir, opts.ManifestID, casMaxChainDepth())
	if err != nil {
		return err
	}

	chunkMap := map[uint64]casResolvedChunk{}
	for _, manifestID := range chain {
		manifestDir := filepath.Join(opts.SourceDir, "snapshots", manifestID)
		manifest, loadErr := loadCASManifest(filepath.Join(manifestDir, "manifest.json"))
		if loadErr != nil {
			return loadErr
		}
		changePath := filepath.Join(manifestDir, manifest.Segments.Change)
		changes, readErr := loadCASChanges(changePath)
		if readErr != nil {
			return readErr
		}
		for _, change := range changes {
			switch change.State {
			case "allocated":
				chunkMap[change.ChunkIndex] = casResolvedChunk{state: change.State, chunkID: change.ChunkID}
			case "freed":
				chunkMap[change.ChunkIndex] = casResolvedChunk{state: change.State}
			default:
				return fmt.Errorf("unknown change state %q", change.State)
			}
		}
	}

	active, err := loadCASActiveIndex(filepath.Join(opts.SourceDir, "indexes", "active.json"))
	if err != nil {
		return err
	}
	locations, err := loadCASChunkLocations(opts.SourceDir, active)
	if err != nil {
		return err
	}

	volumeSize := opts.VolumeSizeBytes
	if volumeSize <= 0 {
		volumeSize = latest.Volume.SizeBytes
	}
	if volumeSize < 0 {
		return fmt.Errorf("invalid volume size: %d", volumeSize)
	}
	chunkSize := opts.ChunkSizeBytes
	if chunkSize <= 0 {
		chunkSize = latest.Volume.ChunkSizeBytes
	}
	if chunkSize <= 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	dst, err := os.OpenFile(filepath.Clean(opts.TargetDevice), os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open target device %q: %w", opts.TargetDevice, err)
	}
	defer dst.Close()

	totalChunks := int64(0)
	if volumeSize > 0 {
		totalChunks = (volumeSize + chunkSize - 1) / chunkSize
	}
	zeroBuf := make([]byte, chunkSize)
	for i := int64(0); i < totalChunks; i++ {
		writeOffset := i * chunkSize
		writeLen := minInt64(chunkSize, volumeSize-writeOffset)
		resolved, ok := chunkMap[uint64(i)]
		if !ok || resolved.state == "freed" {
			if _, err := dst.WriteAt(zeroBuf[:writeLen], writeOffset); err != nil {
				return fmt.Errorf("write zero chunk at %d: %w", writeOffset, err)
			}
			continue
		}

		location, exists := locations[resolved.chunkID]
		if !exists {
			return fmt.Errorf("chunk location not found for chunkID %q", resolved.chunkID)
		}
		payload, err := readCASChunkPayload(opts.SourceDir, location)
		if err != nil {
			return err
		}
		if int64(len(payload)) < writeLen {
			return fmt.Errorf("chunk payload too short for chunkID %q", resolved.chunkID)
		}
		if _, err := dst.WriteAt(payload[:writeLen], writeOffset); err != nil {
			return fmt.Errorf("write chunk payload at %d: %w", writeOffset, err)
		}
	}

	if err := dst.Sync(); err != nil {
		return fmt.Errorf("sync target device: %w", err)
	}
	return nil
}

func validateCASRepoMeta(path string) error {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("read repo metadata %q: %w", path, err)
	}
	var meta casRepoMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("decode repo metadata %q: %w", path, err)
	}
	if meta.RepoVersion != casRepoVersionV1 {
		return fmt.Errorf("unsupported repository version %d", meta.RepoVersion)
	}
	if meta.ChunkHash != "" && meta.ChunkHash != "sha256" {
		return fmt.Errorf("unsupported chunk hash %q", meta.ChunkHash)
	}
	if meta.Compression != "" && meta.Compression != "none" {
		return fmt.Errorf("unsupported repository compression %q", meta.Compression)
	}
	return nil
}

func loadCASManifest(path string) (*casManifest, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", path, err)
	}
	var manifest casManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode manifest %q: %w", path, err)
	}
	if manifest.Segments.Change == "" {
		return nil, fmt.Errorf("manifest %q has empty change segment", path)
	}
	if manifest.Volume.SizeBytes < 0 {
		return nil, fmt.Errorf("manifest %q has invalid volume size %d", path, manifest.Volume.SizeBytes)
	}
	if manifest.Volume.ChunkSizeBytes <= 0 {
		return nil, fmt.Errorf("manifest %q has invalid chunk size %d", path, manifest.Volume.ChunkSizeBytes)
	}
	return &manifest, nil
}

func resolveCASManifestChain(repoPath string, manifestID string, maxDepth int) ([]string, error) {
	if maxDepth <= 0 {
		maxDepth = defaultCASMaxChainDepth
	}
	chain := []string{}
	seen := map[string]struct{}{}
	current := manifestID
	for current != "" {
		if len(chain) >= maxDepth {
			return nil, fmt.Errorf("manifest chain depth exceeds max depth %d", maxDepth)
		}
		if _, ok := seen[current]; ok {
			return nil, fmt.Errorf("manifest parent cycle detected at %q", current)
		}
		seen[current] = struct{}{}
		chain = append(chain, current)

		manifestPath := filepath.Join(repoPath, "snapshots", current, "manifest.json")
		manifest, err := loadCASManifest(manifestPath)
		if err != nil {
			return nil, err
		}
		current = manifest.ParentManifestID
	}

	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

func casMaxChainDepth() int {
	raw := strings.TrimSpace(os.Getenv(envCASMaxChainDepth))
	if raw == "" {
		return defaultCASMaxChainDepth
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return defaultCASMaxChainDepth
	}
	return parsed
}

func loadCASChanges(path string) ([]casChangeRecord, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read change segment %q: %w", path, err)
	}
	changes := []casChangeRecord{}
	if err := json.Unmarshal(data, &changes); err != nil {
		return nil, fmt.Errorf("decode change segment %q: %w", path, err)
	}
	for _, change := range changes {
		if err := validateCASChangeRecord(change); err != nil {
			return nil, fmt.Errorf("invalid change segment %q record: %w", path, err)
		}
	}
	return changes, nil
}

func validateCASChangeRecord(change casChangeRecord) error {
	switch change.State {
	case "allocated":
		if change.ChunkID == "" {
			return fmt.Errorf("allocated chunk %d has empty chunkID", change.ChunkIndex)
		}
	case "freed":
		if change.ChunkID != "" {
			return fmt.Errorf("freed chunk %d must not carry chunkID", change.ChunkIndex)
		}
	default:
		return fmt.Errorf("unknown change state %q", change.State)
	}
	return nil
}

func loadCASActiveIndex(path string) (*casActiveIndex, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read active index: %w", err)
	}
	var active casActiveIndex
	if err := json.Unmarshal(data, &active); err != nil {
		return nil, fmt.Errorf("decode active index: %w", err)
	}
	if active.Version != casIndexVersionV1 {
		return nil, fmt.Errorf("unsupported active index version %d", active.Version)
	}
	return &active, nil
}

func loadCASChunkLocations(repoPath string, active *casActiveIndex) (map[string]casIndexEntry, error) {
	locations := map[string]casIndexEntry{}
	segments := append([]string(nil), active.Segments...)
	sort.Strings(segments)
	for _, segment := range segments {
		segmentPath := filepath.Join(repoPath, "indexes", "segments", segment)
		data, err := os.ReadFile(segmentPath)
		if err != nil {
			return nil, fmt.Errorf("read index segment %q: %w", segment, err)
		}
		entries := []casIndexEntry{}
		if len(data) == 0 {
			continue
		}
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, fmt.Errorf("decode index segment %q: %w", segment, err)
		}
		for _, entry := range entries {
			if err := validateCASIndexEntry(entry); err != nil {
				return nil, fmt.Errorf("invalid index segment %q entry for chunk %q: %w", segment, entry.ChunkID, err)
			}
			locations[entry.ChunkID] = entry
		}
	}
	return locations, nil
}

func readCASChunkPayload(repoPath string, entry casIndexEntry) ([]byte, error) {
	if err := validateCASIndexEntry(entry); err != nil {
		return nil, err
	}

	packPath := filepath.Join(repoPath, "packs", fmt.Sprintf("pack-%06d.pack", entry.PackID))
	pack, err := os.Open(packPath)
	if err != nil {
		return nil, fmt.Errorf("open pack file %q: %w", packPath, err)
	}
	defer pack.Close()

	buf := make([]byte, entry.StoredLength)
	if _, err := pack.Seek(entry.PackOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek pack file %q: %w", packPath, err)
	}
	if _, err := io.ReadFull(pack, buf); err != nil {
		return nil, fmt.Errorf("read pack payload from %q: %w", packPath, err)
	}
	if entry.ChunkID != "" {
		sum := sha256.Sum256(buf)
		if !strings.EqualFold(hex.EncodeToString(sum[:]), entry.ChunkID) {
			return nil, fmt.Errorf("chunk checksum mismatch for chunkID %q", entry.ChunkID)
		}
	}
	return buf, nil
}

func validateCASIndexEntry(entry casIndexEntry) error {
	if entry.ChunkID == "" {
		return fmt.Errorf("chunkID is required")
	}
	if entry.PackID <= 0 {
		return fmt.Errorf("packID must be > 0")
	}
	if entry.PackOffset < 0 {
		return fmt.Errorf("packOffset must be >= 0")
	}
	if entry.StoredLength <= 0 {
		return fmt.Errorf("storedLength must be > 0")
	}
	if entry.RawLength <= 0 {
		return fmt.Errorf("rawLength must be > 0")
	}
	if entry.Codec != "" && entry.Codec != "none" {
		return fmt.Errorf("unsupported chunk codec %q", entry.Codec)
	}
	return nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
