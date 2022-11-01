package mysqldb

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
)

func TestAutoCreateDBOption(t *testing.T) {
	db := &DB{}
	AutoCreateDB()(db)
	assert.True(t, db.autoCreate)
}

func TestDropExistingDBOption(t *testing.T) {
	db := &DB{}
	DropExistingDB()(db)
	assert.True(t, db.dropExisting)
}

func TestWithMigrationsOption(t *testing.T) {
	migrationsFS := fstest.MapFS{
		"migrations/initial.sql": &fstest.MapFile{},
	}
	migrationsDir := "migrations"

	db := &DB{}
	WithMigrations(migrationsFS, migrationsDir)(db)
	assert.Equal(t, migrationsFS, db.migrationsFS)
	assert.Equal(t, migrationsDir, db.migrationsDir)
}

func TestDropDBOnCloseOption(t *testing.T) {
	db := &DB{}
	DropDBOnClose()(db)
	assert.True(t, db.dropOnClose)
}
