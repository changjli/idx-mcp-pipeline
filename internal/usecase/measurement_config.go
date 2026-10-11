package usecase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

const (
	// MeasurementConfigPrefix is the R2 prefix for the Active Filter Set's
	// Config Document. Like the reports, cleanup never evicts it: eviction is
	// raw_files-driven, and no measurement object is a raw_files row.
	MeasurementConfigPrefix = "measurement/config"
	// measurementConfigActiveKey is the mutable pointer object; the versions
	// beside it are immutable and content-addressed.
	measurementConfigActiveKey  = MeasurementConfigPrefix + "/active.json"
	measurementConfigVersionKey = MeasurementConfigPrefix + "/versions/%s.json"
)

// MeasurementConfigDocument is the whole Active Filter Set: every named filter
// set plus the name in force. Read whole on a screen run, so a named set
// resolves in-process and the built-in default still works on an empty bucket.
// Each accepted edit publishes it as a new immutable version, keyed by its hash.
type MeasurementConfigDocument struct {
	Active string                              `json:"active"`
	Sets   map[string]MeasurementFilterSetFile `json:"sets"`
}

// measurementConfigPointer is the mutable pointer: which content-addressed
// version the Active Filter Set currently is.
type measurementConfigPointer struct {
	Hash   string `json:"hash"`
	Active string `json:"active"`
}

// MeasurementConfigStore reads and publishes the Config Document on object
// storage. The Graders read it through MeasurementHarness; the MCP
// get_screener_config / put_screener_config tools (ticket 08) write through it.
type MeasurementConfigStore struct {
	Objects storage.ObjectStore
}

func NewMeasurementConfigStore(objects storage.ObjectStore) *MeasurementConfigStore {
	return &MeasurementConfigStore{Objects: objects}
}

// Registry returns the filter-set registry a screen run grades, the active set's
// name, and the Config Hash to pin in the report. With no published config it
// returns the built-in default registry, active "default", and an empty hash — a
// fresh bucket grades the shipped defaults rather than failing.
func (s *MeasurementConfigStore) Registry(ctx context.Context) (*MeasurementFilterRegistry, string, string, error) {
	doc, hash, err := s.Get(ctx)
	if err != nil {
		return nil, "", "", err
	}
	if doc == nil {
		return newBuiltinMeasurementFilterRegistry(), defaultMeasurementFilterSet, "", nil
	}
	registry, err := doc.registry()
	if err != nil {
		return nil, "", "", err
	}
	return registry, registry.Active(), hash, nil
}

// Get follows the active pointer to the current Config Document and returns it
// with its Config Hash. An empty bucket returns (nil, "", nil): nothing is
// published yet. A pointer that is malformed, dangling, or whose version bytes
// do not hash to the pointer is a structured error — a corrupt config must never
// silently grade the wrong set.
func (s *MeasurementConfigStore) Get(ctx context.Context) (*MeasurementConfigDocument, string, error) {
	if s.Objects == nil {
		return nil, "", fmt.Errorf("config store: no object storage")
	}

	raw, err := s.Objects.GetObject(ctx, measurementConfigActiveKey)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("read config pointer %s: %w", measurementConfigActiveKey, err)
	}

	var pointer measurementConfigPointer
	if err := decodeConfigJSON(raw, &pointer); err != nil {
		return nil, "", fmt.Errorf("%w: %s: %v", ErrInvalidArgument, measurementConfigActiveKey, err)
	}
	if strings.TrimSpace(pointer.Hash) == "" {
		return nil, "", fmt.Errorf("%w: %s names no config version", ErrInvalidArgument, measurementConfigActiveKey)
	}

	versionKey := fmt.Sprintf(measurementConfigVersionKey, pointer.Hash)
	data, err := s.Objects.GetObject(ctx, versionKey)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil, "", fmt.Errorf("%w: %s points at missing version %s", ErrInvalidArgument, measurementConfigActiveKey, pointer.Hash)
	}
	if err != nil {
		return nil, "", fmt.Errorf("read config version %s: %w", pointer.Hash, err)
	}
	if got := contentHash(data); got != pointer.Hash {
		return nil, "", fmt.Errorf("%w: config version %s hashes to %s; the pointer is stale", ErrInvalidArgument, pointer.Hash, got)
	}

	var doc MeasurementConfigDocument
	if err := decodeConfigJSON(data, &doc); err != nil {
		return nil, "", fmt.Errorf("%w: config version %s: %v", ErrInvalidArgument, pointer.Hash, err)
	}
	if _, err := doc.registry(); err != nil {
		return nil, "", err
	}
	return &doc, pointer.Hash, nil
}

// Put publishes doc as a new immutable version and repoints active.json at it,
// returning the Config Hash. Re-publishing identical content is idempotent (the
// same version key holds the same bytes).
func (s *MeasurementConfigStore) Put(ctx context.Context, doc MeasurementConfigDocument) (string, error) {
	if s.Objects == nil {
		return "", fmt.Errorf("config store: no object storage")
	}
	if _, err := doc.registry(); err != nil {
		return "", err
	}

	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal config document: %w", err)
	}
	data = append(data, '\n')
	hash := contentHash(data)

	if err := s.Objects.PutObject(ctx, fmt.Sprintf(measurementConfigVersionKey, hash), data); err != nil {
		return "", fmt.Errorf("write config version %s: %w", hash, err)
	}

	pointer, err := json.Marshal(measurementConfigPointer{Hash: hash, Active: doc.registryActive()})
	if err != nil {
		return "", fmt.Errorf("marshal config pointer: %w", err)
	}
	pointer = append(pointer, '\n')
	if err := s.Objects.PutObject(ctx, measurementConfigActiveKey, pointer); err != nil {
		return "", fmt.Errorf("write config pointer: %w", err)
	}
	return hash, nil
}

// SeedFromDir publishes a Config Document built from the built-in default plus
// every <name>.json under dir, with active naming the set in force. It is the
// one-shot migration from the pre-R2 filter sets (ticket 07b); grading never
// reads dir.
func (s *MeasurementConfigStore) SeedFromDir(ctx context.Context, dir, active string) (string, error) {
	files, err := readMeasurementFilterSetFiles(dir)
	if err != nil {
		return "", err
	}
	sets := make(map[string]MeasurementFilterSetFile, len(files)+1)
	// The built-in default as an empty document entry: every field omitted, so it
	// resolves to the shipped screener defaults — identical to name "default".
	sets[defaultMeasurementFilterSet] = MeasurementFilterSetFile{}
	for name, file := range files {
		sets[name] = file
	}
	return s.Put(ctx, MeasurementConfigDocument{Active: active, Sets: sets})
}

// registry validates the document's shape and builds the filter-set registry it
// describes: the built-in default plus every set, with active in force.
func (d MeasurementConfigDocument) registry() (*MeasurementFilterRegistry, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	registry := newBuiltinMeasurementFilterRegistry()
	for name, file := range d.Sets {
		key := strings.ToLower(strings.TrimSpace(name))
		registry.sets[key] = MeasurementFilterSet{Name: key, Description: file.Description, Params: file.params()}
	}
	registry.active = d.registryActive()
	return registry, nil
}

// registryActive is the active name, defaulting to "default" when unset.
func (d MeasurementConfigDocument) registryActive() string {
	active := strings.ToLower(strings.TrimSpace(d.Active))
	if active == "" {
		return defaultMeasurementFilterSet
	}
	return active
}

// validate rejects a document that could not resolve every name.
func (d MeasurementConfigDocument) validate() error {
	if len(d.Sets) == 0 {
		return fmt.Errorf("%w: config document has no filter sets", ErrInvalidArgument)
	}
	for name := range d.Sets {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: config document has an unnamed filter set", ErrInvalidArgument)
		}
	}
	active := d.registryActive()
	if active == defaultMeasurementFilterSet {
		// The built-in default is always available, even when the document omits
		// a default entry.
		return nil
	}
	for name := range d.Sets {
		if strings.ToLower(strings.TrimSpace(name)) == active {
			return nil
		}
	}
	return fmt.Errorf("%w: config document's active set %q is not in its sets", ErrInvalidArgument, d.Active)
}

// contentHash is the Config Hash: sha256 of the exact document bytes.
func contentHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// decodeConfigJSON decodes strictly: a mistyped field or trailing content is an
// error, never silently ignored.
func decodeConfigJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("unexpected trailing content")
	}
	return nil
}
