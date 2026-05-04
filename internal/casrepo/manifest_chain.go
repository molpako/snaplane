package casrepo

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DefaultManifestChainMaxDepth bounds parent-chain traversal when callers do
// not provide a smaller explicit limit.
const DefaultManifestChainMaxDepth = 1024

// ManifestChainOptions configures manifest parent-chain resolution.
type ManifestChainOptions struct {
	// MaxDepth is the maximum number of manifests that may be visited. Values
	// less than one use DefaultManifestChainMaxDepth.
	MaxDepth int
}

// ResolveManifestChain resolves manifestID's parent chain from repositoryPath.
//
// The returned IDs are ordered from the root ancestor to the requested
// manifest, inclusive. The helper is read-only: it does not initialize or lock
// the repository.
func ResolveManifestChain(repositoryPath string, manifestID string, opts ...ManifestChainOptions) ([]string, error) {
	repoPath := filepath.Clean(repositoryPath)
	if repositoryPath == "" {
		return nil, fmt.Errorf("repository path is required")
	}
	if !filepath.IsAbs(repoPath) {
		return nil, fmt.Errorf("repository path must be absolute: %q", repositoryPath)
	}

	options := ManifestChainOptions{}
	if len(opts) > 0 {
		options = opts[0]
	}
	maxDepth := options.MaxDepth
	if maxDepth <= 0 {
		maxDepth = DefaultManifestChainMaxDepth
	}

	snapshotsDir := filepath.Join(repoPath, "snapshots")
	chain := []string{}
	seen := map[string]struct{}{}
	current := manifestID
	for current != "" {
		currentID, err := validateManifestIDPath(snapshotsDir, current)
		if err != nil {
			return nil, err
		}
		if len(chain) >= maxDepth {
			return nil, fmt.Errorf("manifest chain depth exceeds max depth %d", maxDepth)
		}
		if _, ok := seen[currentID]; ok {
			return nil, fmt.Errorf("manifest parent cycle detected at %q", currentID)
		}
		seen[currentID] = struct{}{}
		chain = append(chain, currentID)

		mf, err := loadManifestForChain(filepath.Join(snapshotsDir, currentID, "manifest.json"))
		if err != nil {
			return nil, err
		}
		current = mf.ParentManifestID
	}

	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

func validateManifestIDPath(baseDir string, manifestID string) (string, error) {
	if manifestID == "" {
		return "", fmt.Errorf("manifest ID is required")
	}
	if filepath.IsAbs(manifestID) {
		return "", fmt.Errorf("manifest ID must be relative: %q", manifestID)
	}
	cleanID := filepath.Clean(manifestID)
	if cleanID == "." || cleanID != manifestID {
		return "", fmt.Errorf("manifest ID %q is not a clean relative path", manifestID)
	}
	manifestPath := filepath.Join(baseDir, cleanID)
	rel, err := filepath.Rel(baseDir, manifestPath)
	if err != nil {
		return "", fmt.Errorf("resolve manifest ID %q: %w", manifestID, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("manifest ID %q escapes repository snapshots", manifestID)
	}
	return cleanID, nil
}

func loadManifestForChain(path string) (*manifest, error) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("read manifest %q: %w", path, err)
	}
	var mf manifest
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, fmt.Errorf("decode manifest %q: %w", path, err)
	}
	return &mf, nil
}
