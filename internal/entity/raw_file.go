package entity

import "time"

type RawFile struct {
	ID            int64      `db:"id"`
	StorageKey    string     `db:"storage_key"`
	Kind          string     `db:"kind"`
	SourceRef     *string    `db:"source_ref"`
	SizeBytes     *int64     `db:"size_bytes"`
	StoredAt      time.Time  `db:"stored_at"`
	DeletedAt     *time.Time `db:"deleted_at"`
	RetentionDays int32      `db:"retention_days"`
}

func (RawFile) TableName() string { return "raw_files" }
