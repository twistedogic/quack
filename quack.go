package quack

import (
	"archive/zip"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/oklog/ulid/v2"
)

func listDir(dir string) ([]string, error) {
	infos, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name())
	}
	sort.Strings(names)
	return names, nil
}

func rotate(root string, n int) error {
	matches, err := listDir(root)
	if err != nil {
		return err
	}
	if len(matches) > n {
		sort.Strings(matches)
		for _, m := range matches[n:] {
			if err := os.Remove(filepath.Join(root, m)); err != nil {
				return err
			}
		}
	}
	return nil
}

func dumpAndZip(ctx context.Context, db *sql.DB, w io.Writer) error {
	dir, err := os.MkdirTemp("", "dump")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if _, err := db.ExecContext(ctx, fmt.Sprintf("EXPORT DATABASE '%s' (FORMAT JSON);", dir)); err != nil {
		return err
	}
	zw := zip.NewWriter(w)
	if err := zw.AddFS(os.DirFS(dir)); err != nil {
		return err
	}
	return zw.Close()
}

func unzipAndLoad(ctx context.Context, db *sql.DB, file string) error {
	dir, err := os.MkdirTemp("", "load")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	zr, err := zip.OpenReader(file)
	if err != nil {
		return err
	}
	defer zr.Close()
	for _, zf := range zr.File {
		r, err := zf.Open()
		if err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(dir, zf.Name))
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, r); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := r.Close(); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("IMPORT DATABASE '%s';", dir)); err != nil {
		return err
	}
	return nil
}

func showTables(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW TABLES;")
	if err != nil {
		return nil, err
	}
	var tables []string
	defer rows.Close()
	var name string
	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func tableExists(ctx context.Context, db *sql.DB, table string) error {
	tables, err := showTables(ctx, db)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if t == table {
			return nil
		}
	}
	return os.ErrNotExist
}

func dedup(ctx context.Context, db *sql.DB, table string) error {
	dedup := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS SELECT DISTINCT * FROM %s", table, table)
	if _, err := db.ExecContext(ctx, dedup); err != nil {
		return err
	}
	return nil
}

func insert(ctx context.Context, db *sql.DB, table string, r io.Reader) error {
	f, err := os.CreateTemp("", "insert")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	if err := tableExists(ctx, db, table); os.IsNotExist(err) {
		stmt := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_json_auto('%s');", table, f.Name())
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		stmt := fmt.Sprintf("COPY %s FROM '%s' (FORMAT json);", table, f.Name())
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

type Client struct {
	mux         sync.Mutex
	dir, prefix string
	n           int

	connecter *duckdb.Connector
	conn      driver.Conn
	db        *sql.DB
}

type Option func(*sql.Conn) error

func New(dir string, n int, options ...Option) (*Client, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path.Join(dir, "snapshot"), 0755); err != nil {
		return nil, err
	}
	c, err := duckdb.NewConnector(filepath.Join(dir, "database.ddb"), nil)
	if err != nil {
		return nil, err
	}
	client := &Client{
		dir:       dir,
		n:         n,
		connecter: c,
		db:        sql.OpenDB(c),
	}
	conn, err := client.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	for _, opt := range options {
		if err := opt(conn); err != nil {
			return nil, err
		}
	}
	return client, nil
}

func (c *Client) RollbackSnapshot(ctx context.Context, n int) error {
	if n > c.n {
		return fmt.Errorf("cannot rollback to last %d snapshot (max: %d)", n, c.n)
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	matches, err := listDir(filepath.Join(c.dir, "snapshot"))
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		return fmt.Errorf("no snapshot to rollback to.")
	}
	tables, err := showTables(ctx, c.db)
	if err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range tables {
		if _, err := tx.QueryContext(ctx, fmt.Sprintf("DROP TABLE %s;", table)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	sort.Strings(matches)
	return unzipAndLoad(ctx, c.db, filepath.Join(c.dir, "snapshot", matches[len(matches)-n]))
}

func (c *Client) Insert(ctx context.Context, table string, r io.Reader) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	return insert(ctx, c.db, table, r)
}

func (c *Client) Query(ctx context.Context, stmt string) (*sql.Rows, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.db.QueryContext(ctx, stmt)
}

func (c *Client) Deduplicate(ctx context.Context, table string) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	return dedup(ctx, c.db, table)
}

func (c *Client) Close(ctx context.Context) error {
	id := ulid.MustNewDefault(time.Now())
	f, err := os.Create(filepath.Join(c.dir, "snapshot", id.String()))
	if err != nil {
		return err
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	if err := dumpAndZip(ctx, c.db, f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := rotate(filepath.Join(c.dir, "snapshot"), c.n); err != nil {
		return err
	}
	if err := c.db.Close(); err != nil {
		return err
	}
	return c.connecter.Close()
}
