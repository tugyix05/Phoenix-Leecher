using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using PhoenixLeecher.Models;

namespace PhoenixLeecher.Services;

/// <summary>Throttle-safe progress tick; applied on the UI thread via IProgress.</summary>
public sealed class EntryTick
{
    public string EntryId { get; set; } = "";
    public Models.EntryStatus Status { get; set; }
    public long DownloadedBytes { get; set; }
    public double Progress { get; set; }
    public double SpeedBps { get; set; }
    public TimeSpan Elapsed { get; set; }
    public string StatusText { get; set; } = "";
}

/// <summary>
/// Download engine (user spec 2026-10-15).
/// Budgets: single connection per file (NO multi-segment), async streaming I/O,
/// max 3 concurrent file downloads (user-changeable), 32KB buffer, stream-to-disk
/// (never whole file in RAM), UI ticks throttled to 4/sec.
/// Fair share (v1.3.8, user spec 2026-11-27): the N file slots are divided
/// equally among the albums currently downloading — 3 slots / 3 albums = 1
/// file each, so 3 rows progress at once; 1 album alone keeps all N slots.
/// Behaviors:
/// - Pause/Resume per entry via HTTP Range (.part files).
/// - Expired/dead direct URL (403/401/410) -> auto re-resolve fresh URL, resume from offset.
/// - pCloud broken -> auto fallback to MediaFire backup (re-resolve + continue same files).
/// - v1.3.9: pCloud resolve failure (stale/empty fileid, API error) -> MediaFire per file;
///   a file with an empty fileid skips pCloud entirely. A file unresolvable on both
///   hosts is skipped and the album falls back to a whole-folder ZIP (getpubzip,
///   code only, no fileid) — that also recovers subfolder contents, which have
///   no fileid by nature and can never download per-file.
/// - Folder = 1 entry: all files into &lt;DownloadRoot&gt;\&lt;folder&gt;\.
/// </summary>
public sealed class DownloadEngine
{
    private const int BufferSize = 32 * 1024;
    private static readonly TimeSpan TickInterval = TimeSpan.FromMilliseconds(250);

    private readonly SemaphoreSlim _fileSlots;
    public int MaxConcurrentFiles { get; }

    // v1.3.8 fair-share state: which albums are downloading, in start order,
    // and how many of their files are in flight right now. All guarded by
    // _fairLock; the pump takes a slot only while below its live share.
    private readonly object _fairLock = new();
    private readonly Dictionary<string, int> _fairSeq = new();
    private readonly Dictionary<string, int> _fairInFlight = new();
    private int _fairNextSeq;

    public DownloadEngine(int maxConcurrentFiles = 3)
    {
        MaxConcurrentFiles = maxConcurrentFiles;
        _fileSlots = new SemaphoreSlim(maxConcurrentFiles, maxConcurrentFiles);
    }

    // Custom download folder (user spec 2026-11-08): default stays
    // %UserProfile%\Downloads\PhoenixLeecher; the user picks any other
    // folder and it is remembered in settings.json (AppSettings).
    // Every path in the app goes through this property, so changing it
    // redirects new downloads, Open Folder and the status bar at once.
    private static string? _customRoot;

    public static string DefaultRoot =>
        Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Downloads", "PhoenixLeecher");

    public static string DownloadRoot => _customRoot ?? DefaultRoot;

    public static void SetDownloadRoot(string? folder) =>
        _customRoot = string.IsNullOrWhiteSpace(folder) ? null : folder;

    public static string Sanitize(string name)
    {
        foreach (char c in Path.GetInvalidFileNameChars()) name = name.Replace(c, '_');
        return name.Trim().TrimEnd('.');
    }

    public async Task DownloadAsync(LeecherEntry entry, IProgress<EntryTick> progress, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var elapsedBase = entry.Elapsed;
        long totalDownloaded = entry.DownloadedBytes;
        long grandTotal = Math.Max(1, entry.TotalSize);
        Report(EntryStatus.Downloading, "Starting…");

        string targetDir = DownloadRoot;
        if (entry.IsFolder) targetDir = Path.Combine(targetDir, Sanitize(entry.DisplayName));
        Directory.CreateDirectory(targetDir);

        var lastTick = DateTime.UtcNow;
        long lastBytes = totalDownloaded;
        var speedSamples = new Queue<(DateTime t, long b)>();

        void Tick(string? note = null)
        {
            var now = DateTime.UtcNow;
            speedSamples.Enqueue((now, totalDownloaded));
            while (speedSamples.Count > 0 && (now - speedSamples.Peek().t).TotalSeconds > 5)
                speedSamples.Dequeue();
            double speed = 0;
            if (speedSamples.Count >= 2)
            {
                var first = speedSamples.Peek();
                var span = (now - first.t).TotalSeconds;
                if (span > 0.2) speed = (totalDownloaded - first.b) / span;
            }
            Report(EntryStatus.Downloading,
                note ?? $"Downloading: {Math.Min(100, totalDownloaded * 100.0 / grandTotal):F0}%",
                speed);
            lastTick = now;
            lastBytes = totalDownloaded;
        }

        void Report(EntryStatus status, string text, double speed = 0)
        {
            progress.Report(new EntryTick
            {
                EntryId = entry.Id,
                Status = status,
                DownloadedBytes = totalDownloaded,
                Progress = Math.Min(100, totalDownloaded * 100.0 / grandTotal),
                SpeedBps = speed,
                Elapsed = elapsedBase + sw.Elapsed,
                StatusText = text,
            });
        }

        FairRegister(entry.Id);
        // v1.3.9: files that cannot be resolved on any host (e.g. a pCloud folder
        // entry has no fileid) are collected here instead of failing the album;
        // after the per-file pump, the album falls back to a whole-folder ZIP.
        var zipNeeded = new ConcurrentBag<string>();
        bool zipTried = false;
        Action<long> addBytes = b => { Interlocked.Add(ref totalDownloaded, b); };
        Action maybeTick = () => { if (DateTime.UtcNow - lastTick >= TickInterval) Tick(); };

        // Last-resort whole-folder ZIP (v1.3.9). Runs at most once per attempt.
        async Task TryZipAsync()
        {
            zipTried = true;
            Report(EntryStatus.Downloading, "Downloading folder as ZIP…");
            await DownloadZipFallbackAsync(entry, targetDir, addBytes, maybeTick, ct);
            long remain = grandTotal - totalDownloaded;
            if (remain > 0) Interlocked.Add(ref totalDownloaded, remain);
        }

        try
        {
            entry.BothLinksFailed = false; // reset per download attempt
            // Build per-file work items from the PRIMARY host.
            var work = await BuildWorkItemsAsync(entry, entry.PrimaryHost, targetDir, ct);

            await PumpFilesAsync(entry, work, targetDir, addBytes, maybeTick, zipNeeded, ct);

            if (!zipNeeded.IsEmpty && entry.PCloudShareUrl != null) await TryZipAsync();

            Report(EntryStatus.Completed, "Completed");
        }
        catch (OperationCanceledException)
        {
            Report(EntryStatus.Paused, $"Paused: {Math.Min(100, totalDownloaded * 100.0 / grandTotal):F0}%");
        }
        catch (Exception ex)
        {
            // v1.3.9: per-file path failed even after the MediaFire fallback ->
            // one whole-folder ZIP attempt (code only, no fileid needed).
            if (entry.PrimaryHost == "pcloud" && entry.PCloudShareUrl != null && !zipTried)
            {
                try
                {
                    await TryZipAsync();
                    Report(EntryStatus.Completed, "Completed");
                    return;
                }
                catch (OperationCanceledException)
                {
                    Report(EntryStatus.Paused, $"Paused: {Math.Min(100, totalDownloaded * 100.0 / grandTotal):F0}%");
                    return;
                }
                catch (Exception zipEx)
                {
                    Report(EntryStatus.Failed, "Failed: " + zipEx.Message);
                    return;
                }
            }
            Report(EntryStatus.Failed, entry.BothLinksFailed ? "Both links failed" : "Failed: " + ex.Message);
        }
        finally
        {
            FairUnregister(entry.Id);
        }
    }

    // ---------- v1.3.8 fair share (user spec 2026-11-27) ----------
    // The N file slots are divided equally among the albums currently
    // downloading. The old code fanned out ALL of one album's files at once
    // and let the FIFO global semaphore hand every slot to the first album,
    // so rows 2-3 sat at "Starting…" until album 1 finished. Now each album
    // launches a file only while below its LIVE share (recomputed as albums
    // start/finish), so 3 rows always progress at once and a lone album
    // still uses all N slots.
    private void FairRegister(string id)
    {
        lock (_fairLock)
        {
            _fairSeq[id] = _fairNextSeq++;
            _fairInFlight[id] = 0;
        }
    }

    private void FairUnregister(string id)
    {
        lock (_fairLock)
        {
            _fairSeq.Remove(id);
            _fairInFlight.Remove(id);
        }
    }

    // Atomically take one file slot for this album if it is below its live
    // fair share: N slots divided among the running albums, remainder spread
    // over the earliest starters (3 slots / 2 albums -> 2 and 1). If more
    // albums run than slots (manual starts past the feeder cap), the first
    // N albums get 1 and the rest 0 until a slot frees.
    private bool FairTryTake(string id)
    {
        lock (_fairLock)
        {
            if (!_fairSeq.TryGetValue(id, out int seq)) return false;
            int count = _fairSeq.Count;
            int @base = MaxConcurrentFiles / count;
            int rem = MaxConcurrentFiles % count;
            int rank = 0;
            foreach (int s in _fairSeq.Values) if (s < seq) rank++;
            int share = @base + (rank < rem ? 1 : 0);
            int inFlight = _fairInFlight.TryGetValue(id, out int n) ? n : 0;
            if (inFlight >= share) return false;
            _fairInFlight[id] = inFlight + 1;
            return true;
        }
    }

    private void FairRelease(string id)
    {
        lock (_fairLock)
        {
            if (_fairInFlight.ContainsKey(id)) _fairInFlight[id]--;
        }
    }

    // Pump one album's files through its live fair share. A file error stops
    // new launches and is rethrown AFTER the in-flight files finish (v1.3.9:
    // the ZIP fallback extracts over the same folder, so it must never race
    // a live download) — the album then reports Failed unless the ZIP
    // fallback recovers it. Cancellation surfaces as Paused via the caller's catch.
    private async Task PumpFilesAsync(LeecherEntry entry, List<FileWork> work, string targetDir,
        Action<long> addBytes, Action maybeTick, ConcurrentBag<string> zipNeeded, CancellationToken ct)
    {
        var running = new List<Task>();
        int next = 0;
        Exception? firstError = null;
        while (next < work.Count || running.Count > 0)
        {
            ct.ThrowIfCancellationRequested();
            while (firstError == null && next < work.Count && FairTryTake(entry.Id))
            {
                var w = work[next++];
                var t = DownloadFileAsync(entry, w, targetDir, addBytes, maybeTick, zipNeeded, ct);
                _ = t.ContinueWith(_ => FairRelease(entry.Id), TaskScheduler.Default);
                running.Add(t);
            }
            if (running.Count == 0)
            {
                // v1.3.9: a file failed -> stop launching, but let the in-flight
                // files finish first so the ZIP fallback (which extracts over
                // the same folder) never races a live download.
                if (firstError != null) break;
                // Share is 0 (more albums than slots): wait for an album to finish.
                await Task.Delay(200, ct);
                continue;
            }
            // Wake when a file finishes, or every 200 ms so a share that grew
            // (another album finished) is picked up promptly.
            await Task.WhenAny(Task.WhenAll(running), Task.Delay(200, ct));
            for (int i = running.Count - 1; i >= 0; i--)
            {
                if (!running[i].IsCompleted) continue;
                var task = running[i];
                running.RemoveAt(i);
                if (task.Exception != null && firstError == null)
                    firstError = task.Exception.InnerException ?? task.Exception;
            }
        }
        if (firstError != null) throw firstError;
    }

    private sealed class FileWork
    {
        public RemoteFile Remote = null!;
        public string LocalPath = "";
        public Func<CancellationToken, Task<string>> ResolveUrl = null!;
    }

    private async Task<List<FileWork>> BuildWorkItemsAsync(LeecherEntry entry, string host, string targetDir, CancellationToken ct)
    {
        var list = new List<FileWork>();
        if (host == "pcloud" && entry.PCloudShareUrl != null)
        {
            string code = ShareResolver.PCloudCodeOf(entry.PCloudShareUrl);
            foreach (var f in entry.Files)
            {
                string fid = f.RefId;
                list.Add(new FileWork
                {
                    Remote = f,
                    LocalPath = Path.Combine(targetDir, Sanitize(f.Name)),
                    ResolveUrl = t => ShareResolver.PCloudDirectUrlAsync(code, fid, entry.Password, t),
                });
            }
        }
        else if (host == "mediafire" && entry.MediaFireShareUrl != null)
        {
            // Re-resolve MediaFire share to map quickkeys by normalized file name.
            var mf = await ShareResolver.ResolveAsync(entry.MediaFireShareUrl, null, ct);
            var byName = mf.Files.ToDictionary(f => MyanmarText.NormalizeKey(f.Name));
            foreach (var f in entry.Files)
            {
                string qk = f.RefId;
                if (byName.TryGetValue(MyanmarText.NormalizeKey(f.Name), out var m)) qk = m.RefId;
                list.Add(new FileWork
                {
                    Remote = f,
                    LocalPath = Path.Combine(targetDir, Sanitize(f.Name)),
                    ResolveUrl = t => ShareResolver.MediaFireDirectUrlAsync(qk, t),
                });
            }
        }
        else throw new InvalidOperationException($"No {host} share URL for entry");
        return list;
    }

    private async Task DownloadFileAsync(LeecherEntry entry, FileWork w, string targetDir,
        Action<long> addBytes, Action maybeTick, ConcurrentBag<string> zipNeeded, CancellationToken ct)
    {
        await _fileSlots.WaitAsync(ct);
        try
        {
            string partPath = w.LocalPath + ".part";
            string finalPath = w.LocalPath;
            // Already fully downloaded in a previous run? skip.
            if (File.Exists(finalPath) && !File.Exists(partPath))
            {
                long fl = new FileInfo(finalPath).Length;
                if (w.Remote.Size <= 0 || fl >= w.Remote.Size)
                {
                    addBytes(0); // no change; entry totals already account via entry.DownloadedBytes
                    return;
                }
                // Incomplete final without .part -> turn into .part and resume.
                File.Move(finalPath, partPath);
            }

            bool fellBack = false;
            // v1.3.9: a pCloud file with no fileid can never resolve (the API
            // demands fileid for folder links) — try MediaFire at once; if
            // there is no backup match, skip the file and let the album-level
            // ZIP fallback recover it instead of failing the whole album.
            if (entry.PrimaryHost == "pcloud" && string.IsNullOrEmpty(w.Remote.RefId))
            {
                if (entry.HasBackup && await TrySwapToMediaFireAsync(entry, w, targetDir, ct))
                    fellBack = true;
                else
                {
                    zipNeeded.Add(w.Remote.Name);
                    return;
                }
            }
            try
            {
                for (int attempt = 0; attempt < 3; attempt++)
                {
                    ct.ThrowIfCancellationRequested();
                    long offset = File.Exists(partPath) ? new FileInfo(partPath).Length : 0;
                    // Already fully downloaded?
                    if (w.Remote.Size > 0 && offset >= w.Remote.Size)
                    {
                        MovePartToFinal(partPath, finalPath);
                        return;
                    }

                    string url;
                    try { url = await w.ResolveUrl(ct); }
                    catch (Exception)
                    {
                        // v1.3.9: resolve failed (stale/empty fileid, pCloud API
                        // error) -> try the MediaFire backup once before retrying.
                        if (!fellBack && entry.HasBackup && entry.PrimaryHost == "pcloud"
                            && await TrySwapToMediaFireAsync(entry, w, targetDir, ct))
                        {
                            fellBack = true;
                            continue;
                        }
                        if (attempt < 2)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(2 << attempt), ct);
                            continue;
                        }
                        throw;
                    }

                    try
                    {
                        long corrected = await StreamToFileAsync(url, partPath, finalPath, offset, addBytes, maybeTick, ct);
                        if (corrected < 0) addBytes(corrected); // server ignored Range: counter correction
                        return; // success
                    }
                    catch (HttpRequestException ex) when (
                        ex.StatusCode is HttpStatusCode.Forbidden or HttpStatusCode.Unauthorized or HttpStatusCode.Gone)
                    {
                        // Expired/dead direct URL -> auto re-resolve fresh URL, resume from offset (user spec).
                        if (attempt == 2) throw;
                        await Task.Delay(TimeSpan.FromSeconds(1), ct);
                        continue;
                    }
                    catch (HttpRequestException) when (!fellBack && entry.HasBackup && entry.PrimaryHost == "pcloud")
                    {
                        // pCloud broken -> auto fallback to MediaFire backup (user spec).
                        fellBack = true;
                        var mfWork = (await BuildWorkItemsAsync(entry, "mediafire", targetDir, ct))
                            .FirstOrDefault(x => MyanmarText.NormalizeKey(x.Remote.Name) == MyanmarText.NormalizeKey(w.Remote.Name));
                        if (mfWork == null) throw;
                        w.ResolveUrl = mfWork.ResolveUrl;
                        continue;
                    }
                }
                throw new HttpRequestException("Download failed after retries");
            }
            catch (Exception) when (fellBack)
            {
                // pCloud failed AND MediaFire backup failed -> notify (user spec 2026-10-16).
                entry.BothLinksFailed = true;
                throw;
            }
        }
        finally
        {
            _fileSlots.Release();
        }
    }

    /// <returns>Negative correction to apply to the byte counter (server ignored Range), else 0.</returns>
    private static async Task<long> StreamToFileAsync(string url, string partPath, string finalPath,
        long offset, Action<long> addBytes, Action maybeTick, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        if (offset > 0) req.Headers.Range = new RangeHeaderValue(offset, null);
        // NOTE: DownloadClient (no overall timeout) — Client's 30s timeout would kill large files.
        using var resp = await HttpRetry.DownloadClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
        if (resp.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable)
        {
            MovePartToFinal(partPath, finalPath); // already complete
            return 0;
        }
        resp.EnsureSuccessStatusCode();

        bool resumed = resp.StatusCode == HttpStatusCode.PartialContent && offset > 0;
        long correction = 0;
        using var fs = new FileStream(partPath, resumed ? FileMode.Append : FileMode.Create,
            FileAccess.Write, FileShare.None, BufferSize, useAsync: true);
        if (!resumed && offset > 0)
        {
            // Server ignored Range: restart from scratch; counter already held `offset` bytes.
            correction = -offset;
            fs.SetLength(0);
        }
        using var stream = await resp.Content.ReadAsStreamAsync(ct);
        var buffer = new byte[BufferSize];
        int read;
        while ((read = await stream.ReadAsync(buffer, ct)) > 0)
        {
            await fs.WriteAsync(buffer.AsMemory(0, read), ct);
            addBytes(read);
            maybeTick();
        }
        await fs.FlushAsync(ct);
        // Dispose the file stream BEFORE renaming: on Windows an open handle
        // (FileShare.None) blocks File.Move, which left a stray .part behind
        // and marked the entry Failed even at 100%.
        await fs.DisposeAsync();
        await stream.DisposeAsync();
        // Completed file -> move .part to final (if we know the size, or always on clean finish)
        MovePartToFinal(partPath, finalPath);
        return correction;
    }

    private static void MovePartToFinal(string partPath, string finalPath)
    {
        if (!File.Exists(partPath)) return;
        if (File.Exists(finalPath)) File.Delete(finalPath);
        File.Move(partPath, finalPath);
    }

    // v1.3.9: swap one file's download URL to its MediaFire backup (re-resolve
    // the MediaFire share live, match by normalized file name — same pattern
    // as the stream-failure fallback). Returns false when there is no backup
    // match or the MediaFire lookup itself fails; never throws except on
    // cancellation, so the caller can fall through to retry / ZIP.
    private async Task<bool> TrySwapToMediaFireAsync(LeecherEntry entry, FileWork w,
        string targetDir, CancellationToken ct)
    {
        try
        {
            var mfWork = (await BuildWorkItemsAsync(entry, "mediafire", targetDir, ct))
                .FirstOrDefault(x => MyanmarText.NormalizeKey(x.Remote.Name) == MyanmarText.NormalizeKey(w.Remote.Name));
            if (mfWork == null) return false;
            w.ResolveUrl = mfWork.ResolveUrl;
            return true;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception) { return false; }
    }

    // v1.3.9: last-resort whole-folder ZIP via pCloud getpubzip. The API needs
    // only the link code (no fileid), so it recovers files the per-file path
    // cannot resolve — including subfolder contents. The zip is streamed to
    // disk (resumable .part, same as a file), verified as a real zip (an API
    // refusal comes back as a JSON error body), extracted into the album
    // folder skipping files already downloaded, then deleted.
    private async Task DownloadZipFallbackAsync(LeecherEntry entry, string targetDir,
        Action<long> addBytes, Action maybeTick, CancellationToken ct)
    {
        string code = ShareResolver.PCloudCodeOf(entry.PCloudShareUrl!);
        string api = "https://api.pcloud.com/getpubzip?code=" + Uri.EscapeDataString(code);
        if (!string.IsNullOrEmpty(entry.Password)) api += "&linkpassword=" + Uri.EscapeDataString(entry.Password);
        string zipPath = Path.Combine(targetDir, ".phoenix-temp.zip");
        string partPath = zipPath + ".part";

        if (!File.Exists(zipPath) || !IsZipFile(zipPath))
        {
            if (File.Exists(zipPath)) File.Delete(zipPath); // stale non-zip (e.g. a JSON error body)
            await _fileSlots.WaitAsync(ct);
            try
            {
                long offset = File.Exists(partPath) ? new FileInfo(partPath).Length : 0;
                long corrected = await StreamToFileAsync(api, partPath, zipPath, offset, addBytes, maybeTick, ct);
                if (corrected < 0) addBytes(corrected);
            }
            finally
            {
                _fileSlots.Release();
            }
            if (!IsZipFile(zipPath))
                throw new HttpRequestException("pCloud zip error: " + await ReadZipErrorAsync(zipPath));
        }

        ExtractZipChecked(zipPath, targetDir);
        File.Delete(zipPath);
    }

    // Extract wrapper: a corrupt zip (right magic, bad body) must not stick
    // around — delete it so the next attempt re-downloads instead of
    // failing extraction forever on the same file.
    private static void ExtractZipChecked(string zipPath, string targetDir)
    {
        try { ExtractZip(zipPath, targetDir); }
        catch { try { File.Delete(zipPath); } catch { } throw; }
    }

    private static bool IsZipFile(string path)
    {
        try
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            byte[] head = new byte[4];
            if (fs.Read(head, 0, 4) < 4) return false;
            return (head[0] == (byte)'P' && head[1] == (byte)'K' && head[2] == 3 && head[3] == 4)
                || (head[0] == (byte)'P' && head[1] == (byte)'K' && head[2] == 5 && head[3] == 6);
        }
        catch (Exception) { return false; }
    }

    private static async Task<string> ReadZipErrorAsync(string path)
    {
        try
        {
            if (new FileInfo(path).Length > 100_000) return "not a zip file";
            string text = await File.ReadAllTextAsync(path);
            if (text.Length > 500) text = text[..500];
            return string.IsNullOrWhiteSpace(text) ? "not a zip file" : text;
        }
        catch (Exception ex) { return ex.Message; }
    }

    // Extract a folder zip into targetDir. getpubzip usually wraps the tree
    // in one top-level folder — strip that single common root when every
    // entry shares it. Skip files already on disk at the same size (they
    // came down per-file already). Zip-slip guard: nothing escapes targetDir.
    private static void ExtractZip(string zipPath, string targetDir)
    {
        using var archive = ZipFile.OpenRead(zipPath);
        var files = archive.Entries.Where(e => !string.IsNullOrEmpty(e.Name)).ToList();
        string? prefix = null;
        var roots = files.Select(e => e.FullName.Split('/')[0]).Distinct().ToList();
        if (roots.Count == 1 && files.All(e => e.FullName.Contains('/')))
            prefix = roots[0] + "/";
        string fullTarget = Path.GetFullPath(targetDir);
        foreach (var e in files)
        {
            string rel = prefix != null && e.FullName.StartsWith(prefix, StringComparison.Ordinal)
                ? e.FullName[prefix.Length..] : e.FullName;
            if (string.IsNullOrEmpty(rel)) continue;
            string dest = Path.GetFullPath(Path.Combine(targetDir, rel.Replace('/', Path.DirectorySeparatorChar)));
            if (!dest.StartsWith(fullTarget + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) continue;
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            if (File.Exists(dest) && new FileInfo(dest).Length == e.Length) continue;
            e.ExtractToFile(dest, overwrite: true);
        }
    }
}
