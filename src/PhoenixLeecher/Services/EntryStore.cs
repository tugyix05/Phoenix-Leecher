using Microsoft.Data.Sqlite;
using System.Text.Json;
using PhoenixLeecher.Models;

namespace PhoenixLeecher.Services;

/// <summary>
/// v1.3.7 SQLite storage for the entry list (replaces entries.json).
/// One indexed table: NormalizedKey is the primary key (the dedup key) and
/// Status is indexed for future queries. Every save runs in a single
/// transaction, so a crash can never leave a half-written file (the old JSON
/// rewrite could). A static gate serializes all DB access in-process —
/// SaveEntries is called from background threads during scans, and SQLite
/// allows only one writer at a time.
/// </summary>
public sealed class EntryStore
{
    private static readonly object Gate = new();
    private readonly string _dbPath;

    public EntryStore(string dbPath) => _dbPath = dbPath;

    public bool DatabaseExists => File.Exists(_dbPath);

    public void EnsureCreated()
    {
        lock (Gate)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_dbPath)!);
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS entries (
                    NormalizedKey TEXT PRIMARY KEY,
                    DisplayName TEXT NOT NULL,
                    IsFolder INTEGER NOT NULL,
                    TotalSize INTEGER NOT NULL,
                    PrimaryHost TEXT NOT NULL,
                    PCloudShareUrl TEXT,
                    MediaFireShareUrl TEXT,
                    Status TEXT NOT NULL,
                    DownloadedBytes INTEGER NOT NULL,
                    FilesJson TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entries_status ON entries (Status);
            ";
            cmd.ExecuteNonQuery();
        }
    }

    private SqliteConnection Open()
    {
        var conn = new SqliteConnection(
            new SqliteConnectionStringBuilder { DataSource = _dbPath }.ToString());
        conn.Open();
        using var pragma = conn.CreateCommand();
        pragma.CommandText = "PRAGMA busy_timeout = 5000;";
        pragma.ExecuteNonQuery();
        return conn;
    }

    public List<SavedEntry> LoadAll()
    {
        lock (Gate)
        {
            EnsureCreated();
            var list = new List<SavedEntry>();
            using var conn = Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT NormalizedKey, DisplayName, IsFolder, TotalSize, PrimaryHost, " +
                "PCloudShareUrl, MediaFireShareUrl, Status, DownloadedBytes, FilesJson FROM entries";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                list.Add(new SavedEntry
                {
                    NormalizedKey = reader.GetString(0),
                    DisplayName = reader.GetString(1),
                    IsFolder = reader.GetInt32(2) != 0,
                    TotalSize = reader.GetInt64(3),
                    PrimaryHost = reader.GetString(4),
                    PCloudShareUrl = reader.IsDBNull(5) ? null : reader.GetString(5),
                    MediaFireShareUrl = reader.IsDBNull(6) ? null : reader.GetString(6),
                    Status = reader.GetString(7),
                    DownloadedBytes = reader.GetInt64(8),
                    Files = JsonSerializer.Deserialize<List<RemoteFile>>(reader.GetString(9)) ?? new(),
                });
            }
            return list;
        }
    }

    public void SaveAll(List<SavedEntry> list)
    {
        lock (Gate)
        {
            EnsureCreated();
            using var conn = Open();
            using var tx = conn.BeginTransaction();
            using (var del = conn.CreateCommand())
            {
                del.Transaction = tx;
                del.CommandText = "DELETE FROM entries";
                del.ExecuteNonQuery();
            }
            foreach (var s in list)
            {
                using var ins = conn.CreateCommand();
                ins.Transaction = tx;
                ins.CommandText = "INSERT INTO entries (NormalizedKey, DisplayName, IsFolder, TotalSize, " +
                    "PrimaryHost, PCloudShareUrl, MediaFireShareUrl, Status, DownloadedBytes, FilesJson) " +
                    "VALUES ($k, $n, $f, $t, $h, $p, $m, $s, $d, $j)";
                ins.Parameters.AddWithValue("$k", s.NormalizedKey);
                ins.Parameters.AddWithValue("$n", s.DisplayName);
                ins.Parameters.AddWithValue("$f", s.IsFolder ? 1 : 0);
                ins.Parameters.AddWithValue("$t", s.TotalSize);
                ins.Parameters.AddWithValue("$h", s.PrimaryHost);
                ins.Parameters.AddWithValue("$p", (object?)s.PCloudShareUrl ?? DBNull.Value);
                ins.Parameters.AddWithValue("$m", (object?)s.MediaFireShareUrl ?? DBNull.Value);
                ins.Parameters.AddWithValue("$s", s.Status);
                ins.Parameters.AddWithValue("$d", s.DownloadedBytes);
                ins.Parameters.AddWithValue("$j", JsonSerializer.Serialize(s.Files));
                ins.ExecuteNonQuery();
            }
            tx.Commit();
        }
    }
}
