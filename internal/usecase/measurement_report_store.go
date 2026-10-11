package usecase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/storage"
)

// MeasurementReportPrefix is the R2 prefix for Grade reports. Like the config,
// cleanup never evicts it: eviction is raw_files-driven, and no report is a
// raw_files row (ADR-0016 amendment).
const MeasurementReportPrefix = "measurement/reports"

// ReportArtifact is one Grade rendered for storage: the ticket key stem and the
// two documents.
type ReportArtifact struct {
	// Base is the report's key stem, e.g. `2026-07-01_to_2026-09-07_volume-tolerant`.
	Base string
	// Version is the sealed version id; empty lets the store mint one.
	Version string
	// JSON and Markdown are the two rendered documents.
	JSON     []byte
	Markdown []byte
}

// newReportArtifact renders one Grade into a ReportArtifact.
func newReportArtifact(base string, value any, markdown string) (ReportArtifact, error) {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return ReportArtifact{}, err
	}
	payload = append(payload, '\n')
	return ReportArtifact{Base: base, JSON: payload, Markdown: []byte(markdown)}, nil
}

// MeasurementReportStore writes and reads Grade reports on object storage. A
// report is sealed at its version key — never overwritten — and the bare
// `<base>.{json,md}` key is always the latest copy, so a re-run (the journal
// Grader's "grade again later" as verdicts resolve) never destroys earlier
// evidence.
type MeasurementReportStore struct {
	Objects storage.ObjectStore
}

func NewMeasurementReportStore(objects storage.ObjectStore) *MeasurementReportStore {
	return &MeasurementReportStore{Objects: objects}
}

// Sealed writes the artifact's immutable version, then updates the bare latest
// keys, returning them. Identical content at a version key is an idempotent
// no-op (re-running migrate is safe); the version id carries a content hash, so
// two runs collapse when their content does.
func (s *MeasurementReportStore) Sealed(ctx context.Context, artifact ReportArtifact) (string, string, error) {
	if s.Objects == nil {
		return "", "", fmt.Errorf("report store: no object storage")
	}
	if artifact.Base == "" {
		return "", "", fmt.Errorf("%w: report has no base name", ErrInvalidArgument)
	}

	version := artifact.Version
	if version == "" {
		version = reportVersionID(artifact.JSON)
	}
	jsonVersionKey := fmt.Sprintf("%s/versions/%s/%s.json", MeasurementReportPrefix, artifact.Base, version)
	mdVersionKey := fmt.Sprintf("%s/versions/%s/%s.md", MeasurementReportPrefix, artifact.Base, version)
	if err := s.putSealed(ctx, jsonVersionKey, artifact.JSON); err != nil {
		return "", "", err
	}
	if err := s.putSealed(ctx, mdVersionKey, artifact.Markdown); err != nil {
		return "", "", err
	}

	jsonKey := fmt.Sprintf("%s/%s.json", MeasurementReportPrefix, artifact.Base)
	mdKey := fmt.Sprintf("%s/%s.md", MeasurementReportPrefix, artifact.Base)
	if err := s.Objects.PutObject(ctx, jsonKey, artifact.JSON); err != nil {
		return "", "", fmt.Errorf("write report %s: %w", jsonKey, err)
	}
	if err := s.Objects.PutObject(ctx, mdKey, artifact.Markdown); err != nil {
		return "", "", fmt.Errorf("write report %s: %w", mdKey, err)
	}
	return jsonKey, mdKey, nil
}

// Get returns the latest JSON and markdown for a base name.
func (s *MeasurementReportStore) Get(ctx context.Context, base string) ([]byte, []byte, error) {
	if s.Objects == nil {
		return nil, nil, fmt.Errorf("report store: no object storage")
	}
	jsonKey := fmt.Sprintf("%s/%s.json", MeasurementReportPrefix, base)
	mdKey := fmt.Sprintf("%s/%s.md", MeasurementReportPrefix, base)
	data, err := s.Objects.GetObject(ctx, jsonKey)
	if err != nil {
		return nil, nil, fmt.Errorf("read report %s: %w", jsonKey, err)
	}
	markdown, err := s.Objects.GetObject(ctx, mdKey)
	if err != nil {
		return nil, nil, fmt.Errorf("read report %s: %w", mdKey, err)
	}
	return data, markdown, nil
}

// List returns the object keys under prefix. It requires the object storage to
// expose listing; a store that cannot list returns an error rather than an
// empty result that would read as "no reports".
func (s *MeasurementReportStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.Objects == nil {
		return nil, fmt.Errorf("report store: no object storage")
	}
	lister, ok := s.Objects.(interface {
		ListObjects(ctx context.Context, prefix string) ([]string, error)
	})
	if !ok {
		return nil, fmt.Errorf("report store: object storage cannot list")
	}
	return lister.ListObjects(ctx, prefix)
}

// putSealed writes a version object once. An identical existing object is a
// no-op; a differing object under the same key is refused, so sealed evidence is
// never overwritten.
func (s *MeasurementReportStore) putSealed(ctx context.Context, key string, data []byte) error {
	existing, err := s.Objects.GetObject(ctx, key)
	switch {
	case err == nil:
		if bytes.Equal(existing, data) {
			return nil
		}
		return fmt.Errorf("%w: sealed report %s already exists with different content", ErrInvalidArgument, key)
	case errors.Is(err, storage.ErrObjectNotFound):
		if err := s.Objects.PutObject(ctx, key, data); err != nil {
			return fmt.Errorf("write report %s: %w", key, err)
		}
		return nil
	default:
		return fmt.Errorf("read report %s: %w", key, err)
	}
}

// reportVersionID is a content-addressed version id: identical report content
// maps to one sealed version, so re-running a run or the migration is a no-op.
func reportVersionID(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}
