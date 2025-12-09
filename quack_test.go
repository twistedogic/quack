package quack

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func expectSnapshots(t *testing.T, root string, n int) {
	t.Helper()
	files, err := listDir(filepath.Join(root, "snapshot"))
	require.NoError(t, err)
	require.Len(t, files, n)
}

func Test_Client(t *testing.T) {
	dir, err := os.MkdirTemp("", "test_quack")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	t.Run("init table", func(t *testing.T) {
		client, err := New(dir, 3)
		require.NoError(t, err)
		require.NoError(t, client.Insert(t.Context(), "table_a", bytes.NewBuffer([]byte(`{"name":"a", "value":10}`))))
		require.NoError(t, client.Close(t.Context()))
		expectSnapshots(t, dir, 1)
	})
	t.Run("repeat", func(t *testing.T) {
		client, err := New(dir, 3)
		require.NoError(t, err)
		require.NoError(t, client.Insert(t.Context(), "table_a", bytes.NewBuffer([]byte(`{"name":"a", "value":10}`))))
		require.NoError(t, client.Close(t.Context()))
		expectSnapshots(t, dir, 2)
	})
	t.Run("again", func(t *testing.T) {
		client, err := New(dir, 3)
		require.NoError(t, err)
		require.NoError(t, client.Insert(t.Context(), "table_a", bytes.NewBuffer([]byte(`{"name":"a", "value":10}`))))
		require.NoError(t, client.Close(t.Context()))
		expectSnapshots(t, dir, 3)
	})
	t.Run("should rotate snapshot", func(t *testing.T) {
		client, err := New(dir, 3)
		require.NoError(t, err)
		require.NoError(t, client.Insert(t.Context(), "table_a", bytes.NewBuffer([]byte(`{"name":"a", "value":10}`))))
		require.NoError(t, client.Close(t.Context()))
		expectSnapshots(t, dir, 3)
	})
	t.Run("query", func(t *testing.T) {
		client, err := New(dir, 3)
		require.NoError(t, err)
		rows, err := client.Query(t.Context(), "select * from table_a;")
		count := 0
		for rows.Next() {
			count += 1
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, 4, count)
	})
	t.Run("dedup", func(t *testing.T) {
		client, err := New(dir, 2)
		require.NoError(t, err)
		require.NoError(t, client.Deduplicate(t.Context(), "table_a"))
		rows, err := client.Query(t.Context(), "select * from table_a;")
		count := 0
		for rows.Next() {
			count += 1
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, 1, count)
	})
	t.Run("rollback", func(t *testing.T) {
		client, err := New(dir, 2)
		require.NoError(t, err)
		require.NoError(t, client.RollbackSnapshot(t.Context(), 1))
		rows, err := client.Query(t.Context(), "select * from table_a;")
		count := 0
		for rows.Next() {
			count += 1
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, 3, count)
	})
}
