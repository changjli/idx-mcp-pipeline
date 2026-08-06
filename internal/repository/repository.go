package repository

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Tabler returns the table name for an entity.
type Tabler interface {
	TableName() string
}

// Repository provides generic CRUD operations for any entity.
// Concrete repos embed this and add domain-specific queries.
type Repository[T any] struct{}

// FindByID retrieves an entity by its BIGSERIAL id column.
// Requires the entity to implement Tabler.
func (r *Repository[T]) FindByID(db *sqlx.DB, id int64) (*T, error) {
	var t T
	table, err := tableName(&t)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", table)
	if err := db.Get(&t, query, id); err != nil {
		return nil, err
	}
	return &t, nil
}

// DeleteByID removes an entity by its BIGSERIAL id column.
func (r *Repository[T]) DeleteByID(db *sqlx.DB, id int64) error {
	var t T
	table, err := tableName(&t)
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = $1", table), id)
	return err
}

// Count returns the total number of rows in the table.
func (r *Repository[T]) Count(db *sqlx.DB) (int64, error) {
	var t T
	table, err := tableName(&t)
	if err != nil {
		return 0, err
	}
	var count int64
	err = db.Get(&count, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	return count, err
}

func tableName(t interface{}) (string, error) {
	tabler, ok := t.(Tabler)
	if !ok {
		return "", fmt.Errorf("%T does not implement repository.Tabler", t)
	}
	return tabler.TableName(), nil
}
