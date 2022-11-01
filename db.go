package mysqldb

import (
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	// mysql driver
	"github.com/go-sql-driver/mysql"
)

// DB wraps a SQL database with specific functionality
type DB struct {
	db            *sql.DB
	name          string
	dsn           string
	autoCreate    bool
	dropExisting  bool
	migrationsDir string
	migrationsFS  fs.FS
	dropOnClose   bool
}

func (db *DB) BeginTx() (*Tx, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx: tx,
	}, nil
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args)
}

func (db *DB) Query(query string, args ...interface{}) (Rows, error) {
	return db.db.Query(query, args)
}

func (db *DB) QueryRow(query string, args ...interface{}) Row {
	return db.db.QueryRow(query, args)
}

// Tx wraps a sql Tx.
type Tx struct {
	tx *sql.Tx
}

func (tx *Tx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.tx.Exec(query, args)
}

func (tx *Tx) Query(query string, args ...interface{}) (Rows, error) {
	return tx.tx.Query(query, args)
}

func (tx *Tx) QueryRow(query string, args ...interface{}) Row {
	return tx.tx.QueryRow(query, args)
}

type Scanner interface {
	Scan(dest ...interface{}) error
}

type Row interface {
	Scanner
}

type Rows interface {
	Row
	Next() bool
	Close() error
}

// Conn is used as a common interface for DB and DBWithTx.
// This allows stores to not worry about whether or not
// there's an active transaction.
type Conn interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (Rows, error)
	QueryRow(query string, args ...interface{}) Row
}

// Option is an option to be applied to the DB.
type Option func(*DB)

// AutoCreateDB returns an option that will configure the DB to
// automatically create the database in the provided instance
// if it doesn't exist already.
func AutoCreateDB() Option {
	return func(db *DB) {
		db.autoCreate = true
	}
}

// DropExistingDB returns an option that will configure the DB to
// drop the database if it currently exists. This would be useful if
// the DB needs dropped before using the AutoCreateDB Option.
func DropExistingDB() Option {
	return func(db *DB) {
		db.dropExisting = true
	}
}

// WithMigrations returns an option that will configure the DB to
// perform automatic migrations. No subdirectories will be searched,
// and only files with a `.sql` extension will be run. The migration
// files are sorted by filename and then executed in that order.
// If the directory string provided is empty, no migrations will be run.
func WithMigrations(migrationsFS fs.FS, migrationsDir string) Option {
	return func(db *DB) {
		db.migrationsFS = migrationsFS
		db.migrationsDir = migrationsDir
	}
}

// DropDBOnClose returns an option that will configure the DB to
// drop the underlying database when the DB is closed. This is useful
// if the database is only needed temporarily e.g. for testing.
func DropDBOnClose() Option {
	return func(db *DB) {
		db.dropOnClose = true
	}
}

// NewDB returns a new DB with any necessary actions from the given options performed.
func NewDB(dsn string, options ...Option) (*DB, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing dsn: %w", err)
	}

	d := &DB{name: cfg.DBName, dsn: dsn}
	for _, o := range options {
		o(d)
	}

	if d.dropExisting {
		cfg.DBName = ""
		if err = dropExistingDatabaseIfExist(cfg.FormatDSN(), d.name); err != nil {
			return nil, fmt.Errorf("dropping existing database: %w", err)
		}
		cfg.DBName = d.name
	}

	if d.autoCreate {
		cfg.DBName = ""
		if err = createDatabaseIfNotExist(cfg.FormatDSN(), d.name); err != nil {
			return nil, fmt.Errorf("auto-creating database: %w", err)
		}
		cfg.DBName = d.name
	}

	d.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if err = d.db.Ping(); err != nil {
		d.db.Close()
		return nil, err
	}

	if d.migrationsDir != "" {
		if err = d.runMigrations(); err != nil {
			d.db.Close()
			return nil, fmt.Errorf("running migrations: %w", err)
		}
	}

	return d, nil
}

// Close handles closing any underlying resources for the database. It also
// runs any of the specified options that are required at the end of the database's
// use, such as DropDBOnClose.
func (db *DB) Close() error {
	err := db.db.Close()
	if err != nil {
		return fmt.Errorf("closing database: %w", err)
	}

	if !db.dropOnClose {
		return nil
	}

	cfg, err := mysql.ParseDSN(db.dsn)
	if err != nil {
		return fmt.Errorf("parsing dsn: %w", err)
	}

	cfg.DBName = ""
	return dropExistingDatabaseIfExist(cfg.FormatDSN(), db.name)
}

func createDatabaseIfNotExist(dsn, dbName string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	_, err = db.Exec(`CREATE DATABASE IF NOT EXISTS ` + dbName + `;`)
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	return nil
}

func dropExistingDatabaseIfExist(dsn, dbName string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	_, err = db.Exec(`DROP DATABASE IF EXISTS ` + dbName + `;`)
	if err != nil {
		return fmt.Errorf("dropping database: %w", err)
	}

	return nil
}

func (db *DB) runMigrations() error {
	_, err := db.db.Exec(`
CREATE TABLE IF NOT EXISTS __Migrations (
	ID INT NOT NULL AUTO_INCREMENT,
	` + "`" + `Name` + "`" + ` VARCHAR(255) NOT NULL,
	RunAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY(ID)
);`)
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}

	var entries []fs.DirEntry
	entries, err = fs.ReadDir(db.migrationsFS, db.migrationsDir)
	if err != nil {
		return fmt.Errorf("reading migrations directory: %w", err)
	}

	migrations := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || strings.ToLower(path.Ext(entry.Name())) != ".sql" {
			continue
		}

		migrations = append(migrations, entry.Name())
	}

	sort.Strings(migrations)

	for _, migration := range migrations {
		var exists Bool
		row := db.db.QueryRow("SELECT COALESCE((SELECT b'1' FROM __Migrations WHERE `Name` = ?), b'0');", migration)
		if err = row.Scan(&exists); err != nil {
			return fmt.Errorf("querying for migration: %w", err)
		}

		if exists {
			continue
		}

		p := path.Join(db.migrationsDir, migration)
		s, err := fs.ReadFile(db.migrationsFS, p)
		if err != nil {
			return fmt.Errorf("reading file %s: %w", p, err)
		}
		sql := strings.TrimSpace(string(s))

		delim := ";"
		for sql != "" {
			nextDelimIndex := strings.Index(sql, delim)
			nextDelimChangeIndex := strings.Index(sql, "delimiter ")

			if nextDelimIndex == -1 && nextDelimChangeIndex == -1 {
				return fmt.Errorf("unexpected end of migration: %s", migration)
			}

			if nextDelimChangeIndex == -1 || (nextDelimIndex != -1 && nextDelimIndex < nextDelimChangeIndex) {
				var stmt string
				// only include the delimiter if it's a semi-colon
				if delim == ";" {
					stmt = sql[:nextDelimIndex+1]
				} else {
					stmt = sql[:nextDelimIndex]
				}

				if _, err = db.db.Exec(stmt); err != nil {
					return fmt.Errorf("executing migration statement: %w", err)
				}

				if len(sql) <= nextDelimIndex {
					break
				}
				sql = strings.TrimSpace(sql[nextDelimIndex+1:])

				continue
			}

			delimLineEndIndex := strings.Index(sql, "\n")
			if delimLineEndIndex == -1 {
				// there's nothing after this delimiter change, so we're done with the script
				break
			}

			delim = strings.Replace(sql[:delimLineEndIndex+1], "delimiter ", "", 1)
			delim = strings.Replace(delim, "\n", "", -1)
			delim = strings.TrimSpace(delim)

			// advance the sql past the delimiter change statement since the client will
			// only handle this correctly without it, or break if it's the end of the script
			if len(sql) <= delimLineEndIndex {
				break
			}
			sql = strings.TrimSpace(sql[delimLineEndIndex+1:])
		}

		_, err = db.db.Exec("INSERT INTO __Migrations(`Name`) VALUES (?);", migration)
		if err != nil {
			return fmt.Errorf("inserting migration record '%s': %w", migration, err)
		}
	}

	return nil
}

func NewNullTime(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Valid: true, Time: *t}
}

type Bool bool

func (b *Bool) Scan(src interface{}) error {
	tmp, ok := src.([]uint8)
	if !ok {
		return fmt.Errorf("unexpected type for mysqlBool: %T", src)
	}
	switch string(tmp) {
	case "\x00":
		v := Bool(false)
		*b = v
	case "\x01":
		v := Bool(true)
		*b = v
	}
	return nil
}

func (b Bool) Value() interface{} {
	if b {
		return []uint8("\x01")
	}
	return []uint8("\x00")
}
