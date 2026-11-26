using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;
using PhoenixLeecher.Models;
using PhoenixLeecher.Services;

namespace PhoenixLeecher;

public partial class MainWindow : Window
{
    private readonly ConcurrentDictionary<string, LeecherEntry> _entries = new();
    private readonly ObservableCollection<LeecherEntry> _rows = new();
    private readonly Dictionary<string, LeecherEntry> _byId = new();
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _dlCts = new();
    private readonly ConcurrentDictionary<string, Task> _dlTasks = new();
    private bool _isBatchRunning;
    private readonly List<string> _bothFailedNames = new(); // guarded by lock(_bothFailedNames)
    private readonly Dictionary<string, string> _passwords = new();
    private readonly LinkScanner _scanner = new();
    private int _lastScanSavedCount; // throttle SaveEntries during Whole scan
    private DownloadEngine _engine = new(maxConcurrentFiles: 3);
    private CancellationTokenSource? _scanCts;
    private CancellationTokenSource? _dlBatchCts; // Download All batch token; Pause All cancels it too
    private CancellationTokenSource? _pipeCts; // Scan & Download pipeline token (same object as _dlBatchCts while running)
    private bool _pipeIsResume; // set by BeginWholeScanAsync: this press resumes an earlier run (user spec 2026-11-26)
    private bool _scanRunning;
    private static string EntriesPath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "PhoenixLeecher", "entries.json");
    // v1.3.7: the list lives in SQLite now (entries.db); entries.json is only
    // the one-time migration source, kept afterwards as entries.json.bak.
    private static string DbPath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "PhoenixLeecher", "entries.db");
    private readonly IProgress<EntryTick> _tickProgress;
    private readonly IProgress<ScanProgress> _scanProgress;
    private readonly DispatcherTimer _syncTimer = new();

    public MainWindow()
    {
        InitializeComponent();
        var v = System.Reflection.Assembly.GetExecutingAssembly().GetName().Version;
        Title = $"Phoenix Leecher v{v?.Major}.{v?.Minor}.{v?.Build}";
        Grid.ItemsSource = _rows;

        _tickProgress = new Progress<EntryTick>(ApplyTick);
        _scanProgress = new Progress<ScanProgress>(ApplyScanProgress);

        _syncTimer.Interval = TimeSpan.FromMilliseconds(500);
        _syncTimer.Tick += (_, _) => SyncRows();
        _syncTimer.Start();

        // Custom download folder (user spec 2026-11-08): apply the saved
        // folder before anything reads DownloadRoot (status bar, LoadEntries).
        DownloadEngine.SetDownloadRoot(AppSettings.LoadDownloadFolder());
        StatusFolder.Text = DownloadEngine.DownloadRoot;
        LoadEntries(); // restore list from previous session (no auto-download)
        UpdateCounts();
        Closing += (_, _) => SaveEntries();
    }

    // ---------- rows sync (UI thread) ----------
    private void RenumberRows()
    {
        for (int i = 0; i < _rows.Count; i++)
            _rows[i].RowNumber = i + 1;
    }

    private void SyncRows()
    {
        foreach (var kv in _entries)
        {
            if (!_byId.ContainsKey(kv.Value.Id))
            {
                _byId[kv.Value.Id] = kv.Value;
                // User spec 2026-10-17: don't show Completed in the visible list.
                // v1.3.5: Deleted entries are never shown either.
                // v1.3.5: Dead entries are never shown either (user: don't include Dead in scans).
                if (kv.Value.Status != EntryStatus.Completed && kv.Value.Status != EntryStatus.Deleted && kv.Value.Status != EntryStatus.Dead)
                    _rows.Add(kv.Value);
            }
        }
        // removals: a row whose entry left _entries (v1.3.5 folder-vs-file
        // suppression removes the file entry from _entries) must leave _rows too.
        var liveIds = new HashSet<string>(_entries.Values.Select(e => e.Id));
        for (int i = _rows.Count - 1; i >= 0; i--)
            if (!_byId.ContainsKey(_rows[i].Id) || !liveIds.Contains(_rows[i].Id))
            {
                _byId.Remove(_rows[i].Id);
                _rows.RemoveAt(i);
            }
        RenumberRows();
        UpdateCounts();
    }

    private void UpdateCounts()
    {
        int done = _rows.Count(e => e.Status == EntryStatus.Completed);
        double totalSpeed = _rows.Where(e => e.Status == EntryStatus.Downloading).Sum(e => e.SpeedBps);
        StatusCounts.Text = $"Complete downloads {done} of {_rows.Count}";
        StatusSpeed.Text = totalSpeed > 0 ? $"Current speed: {FormatSpeed(totalSpeed)}" : "";
        EmptyState.Visibility = _rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    private static string FormatSpeed(double bps)
    {
        string[] u = { "B", "KB", "MB", "GB" };
        double d = bps; int i = 0;
        while (d >= 1024 && i < u.Length - 1) { d /= 1024; i++; }
        return $"{d:F1} {u[i]}/s";
    }

    private void ApplyTick(EntryTick t)
    {
        if (!_byId.TryGetValue(t.EntryId, out var e)) return;
        // v1.3.5: a Deleted entry never changes status again — but a late tick
        // from a download cancelled by the delete must still release its CTS,
        // or ChangeFolder would stay blocked on a dead download.
        if (e.Status == EntryStatus.Deleted)
        {
            if (t.Status is EntryStatus.Completed or EntryStatus.Failed or EntryStatus.Paused)
                if (_dlCts.Remove(t.EntryId, out var deadCts)) deadCts.Dispose();
            return;
        }
        e.Status = t.Status;
        e.DownloadedBytes = t.DownloadedBytes;
        e.Progress = t.Progress;
        e.SpeedBps = t.SpeedBps;
        e.Elapsed = t.Elapsed;
        e.StatusText = t.StatusText;
        if (t.Status is EntryStatus.Completed or EntryStatus.Failed or EntryStatus.Paused)
        {
            if (_dlCts.Remove(t.EntryId, out var cts)) cts.Dispose();
            // Both-links-failed: pCloud tried, MediaFire backup tried, both failed (user spec 2026-10-16).
            if (t.Status == EntryStatus.Failed && e.BothLinksFailed)
            {
                lock (_bothFailedNames) _bothFailedNames.Add(e.DisplayName);
                if (!_isBatchRunning)
                {
                    // Single download: notify immediately.
                    MessageBox.Show($"Album '{e.DisplayName}': pCloud and MediaFire links both failed.",
                        "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Warning);
                    lock (_bothFailedNames) _bothFailedNames.Clear();
                }
                // Batch: DownloadAll_Click shows one summary at the end.
            }
            SaveEntries();
        }
        UpdateCounts();
    }

    private void ApplyScanProgress(ScanProgress p)
    {
        ScanBar.Maximum = p.Target;
        ScanBar.Value = p.AlbumsFound;
        // Persist scan finds promptly (every 10) so a crash/kill doesn't lose dedup memory.
        if (p.Running && p.AlbumsFound >= _lastScanSavedCount + 10)
        {
            _lastScanSavedCount = p.AlbumsFound;
            SaveEntries();
        }
        ScanText.Text = p.Running
            ? $"Scanning older posts… {p.AlbumsFound}/{p.Target} albums" +
              (p.TotalPosts > 0 ? $" · post {p.PostsDone}/{p.TotalPosts}" : $" · {p.PostsDone} posts")
            : $"Scan {p.Note}: {p.AlbumsFound}/{p.Target} albums";
        if (!p.Running)
        {
            ScanBtn.IsEnabled = true;
            StopBtn.IsEnabled = false;
            // Target reached -> cursor preserved + counter reset by Scanner (next Whole scan
            // finds the NEXT 50). Exhausted -> clear (fresh start may find new posts).
            // Stopped early -> keep for Resume.
            if (p.AlbumsFound >= p.Target)
            {
                ScanBtnLabel.Text = "Whole scan";
            }
            else if (p.Note.Contains("exhaust"))
            {
                _scanner.ClearState(UrlBox.Text.Trim());
                ScanBtnLabel.Text = "Whole scan";
            }
            else if (LinkScanner.IsBlogspot(UrlBox.Text.Trim()))
            {
                ScanBtnLabel.Text = $"Resume scan ({p.AlbumsFound}/{p.Target})";
            }
        }
    }

    private string CurrentUrl => UrlBox.Text.Trim();

    // ---------- Find Links (immediate, page only) ----------
    // ---------- Find Links (immediate, page only). Returns new unique albums found. ----------
    private async Task<int> DoFindLinksAsync()
    {
        string url = CurrentUrl; // capture on UI thread: UrlBox cannot be read from a worker thread
        if (string.IsNullOrWhiteSpace(url)) return 0;
        // Direct link support (user request 2026-10-16): if the pasted URL IS a
        // pCloud/MediaFire share link, resolve it directly instead of fetching as a page.
        if (LinkScanner.IsShareLink(url))
        {
            var (entry, isNew) = await Task.Run(() => _scanner.ResolveAndAddAsync(url, _entries, null, CancellationToken.None));
            // v1.3.5: Dead entries are never listed — but a directly pasted dead
            // link still gets its "Link dead" feedback here.
            if (entry.Status == EntryStatus.Dead)
                MessageBox.Show("Link dead: " + url, "Phoenix Leecher",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
            SyncRows();
            return isNew ? 1 : 0;
        }
        var links = await Task.Run(() => _scanner.FindLinksAsync(url, CancellationToken.None));
        int newAlbums = 0;
        await Task.Run(async () =>
        {
            foreach (var link in links)
            {
                var (_, isNew) = await _scanner.ResolveAndAddAsync(link, _entries, null, CancellationToken.None);
                if (isNew) { newAlbums++; SaveEntries(); }
            }
        });
        SyncRows();
        return newAlbums;
    }

    // ---------- Page scan (immediate, page only) ----------
    private async void PageScan_Click(object sender, RoutedEventArgs e)
    {
        if (_scanRunning) return;
        SetScanRunning(true);
        _lastScanSavedCount = 0;
        try { await DoFindLinksAsync(); }
        catch (Exception ex)
        {
            MessageBox.Show("Page scan failed: " + ex.Message, "Phoenix Leecher",
                MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        finally { SaveEntries(); SetScanRunning(false); }
    }

    // ---------- Whole scan (page + older posts, 50 NEW albums, resumable) ----------
    private async void WholeScan_Click(object sender, RoutedEventArgs e)
    {
        if (_scanRunning) return;
        string url = CurrentUrl; // capture on UI thread: UrlBox cannot be read from a worker thread
        if (string.IsNullOrWhiteSpace(url)) return;
        SetScanRunning(true);
        _lastScanSavedCount = 0;
        try
        {
            var scanTask = await BeginWholeScanAsync(url, LinkScanner.LinkTarget);
            await scanTask;
        }
        catch (OperationCanceledException) { /* Stop pressed: state already saved */ }
        catch (Exception ex)
        {
            MessageBox.Show("Scan failed: " + ex.Message, "Phoenix Leecher",
                MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        finally
        {
            SyncRows();
            SaveEntries();
            _scanCts?.Dispose();
            _scanCts = null;
            SetScanRunning(false);
        }
    }

    // Starts a Whole scan (page links first, then feed crawl to <target> NEW
    // albums) and returns the running scan task. The caller awaits it and owns
    // the try/catch/finally (SyncRows/SaveEntries/dispose/SetScanRunning).
    // Run model (user spec 2026-10-16):
    // - Fresh: page links first (counted), then older-posts crawl to <target> NEW albums.
    // - Stopped early: resume from saved cursor, no page re-scrape.
    // - Target reached: NEW run — cursor preserved, counter reset, next <target>
    //   NEW albums; already-seen links are not re-counted (dedup).
    private async Task<Task> BeginWholeScanAsync(string url, int target)
    {
        var saved = _scanner.LoadState(url);
        bool isBlog = LinkScanner.IsBlogspot(url);
        bool targetReached = saved != null && saved.AlbumsFound >= target;
        bool isResume = saved != null && !targetReached && saved.AlbumsFound > 0 && isBlog;

        if (targetReached)
        {
            // Previous run completed <target>: reset counter, keep cursor for the next run.
            saved!.AlbumsFound = 0;
            _scanner.SaveState(saved);
            isResume = true; // skip page re-scrape, continue from cursor
        }

        if (!isResume)
        {
            int pageAlbums = await DoFindLinksAsync();
            // Seed the run counter with page albums so the feed crawl continues toward the target.
            var s = _scanner.LoadState(url) ?? new ScanState { BlogUrl = url };
            s.AlbumsFound = pageAlbums;
            s.StartIndex = 1;
            _scanner.SaveState(s);
        }

        _pipeIsResume = isResume; // Scan & Download feeder reads this: a resumed run downloads all Ready rows
        _scanCts = new CancellationTokenSource();
        StopBtn.IsEnabled = true;
        ScanStrip.Visibility = Visibility.Visible;
        return Task.Run(() => _scanner.ScanAsync(url, _entries, _scanProgress, _scanCts.Token, target));
    }

    // ---------- Scan & Download (user spec 2026-11-26) ----------
    // The number field is how many NEW albums to scan for. Pressing the button
    // scans first and downloads each album as soon as it is found — scan and
    // download run at the same time (pipelined), downloads N at a time per
    // Max concurrent. Stop ends the scan (no new downloads); Pause All ends
    // everything. NOTE: pressing Download All mid-pipeline cancels this batch
    // token and stops the scan — Download All then takes over the downloads.
    // Re-press (user spec 2026-11-26): pressing again while a run is stopped
    // resumes the SAME run — scan continues from the saved cursor to the field's
    // number, and the feeder downloads the unfinished Ready rows too.
    private async void ScanDownload_Click(object sender, RoutedEventArgs e)
    {
        if (_scanRunning || _isBatchRunning) return;
        string url = CurrentUrl; // capture on UI thread
        if (string.IsNullOrWhiteSpace(url)) return;
        if (!int.TryParse(ScanCountBox.Text.Trim(), out int target) || target < 1) target = 50;
        if (target > 1000) target = 1000; // safety bound: the feed is finite, a runaway target scans forever
        SetScanRunning(true);
        _lastScanSavedCount = 0;
        _pipeCts = new CancellationTokenSource();
        _dlBatchCts = _pipeCts; // Pause All cancels this: stops the feeder and the downloads
        _isBatchRunning = true;
        // Fresh press: only download what THIS scan finds — rows already sitting
        // in the list before the button was pressed are left alone (Download All
        // is for those). Re-press (resume, user spec 2026-11-26): the feeder also
        // downloads the unfinished Ready rows the earlier run left behind —
        // nothing is left behind.
        var knownIds = new HashSet<string>(_rows.Select(e => e.Id));
        Task? scanTask = null;
        try
        {
            scanTask = await BeginWholeScanAsync(url, target);
            while (true)
            {
                _pipeCts.Token.ThrowIfCancellationRequested();
                if (_scanCts?.IsCancellationRequested == true) break; // Stop pressed: no new downloads
                // Feed: start Ready entries while a download slot is free.
                foreach (var entry in _rows.Where(x => x.Status == EntryStatus.Ready && (_pipeIsResume || !knownIds.Contains(x.Id))).ToList())
                {
                    if (_dlTasks.Count >= _engine.MaxConcurrentFiles) break;
                    _pipeCts.Token.ThrowIfCancellationRequested();
                    StartDownloadNow(entry); // false if already running / needs password — skip silently
                }
                if (scanTask.IsCompleted && _dlTasks.Count == 0) break;
                // When the scan is done but downloads remain, wait on the delay
                // alone — WhenAny with an already-completed scan task would return
                // at once and spin this loop with no pause.
                if (scanTask.IsCompleted)
                    await Task.Delay(500, _pipeCts.Token);
                else
                    await Task.WhenAny(scanTask, Task.Delay(500, _pipeCts.Token));
            }
            await scanTask; // surface scan errors
        }
        catch (OperationCanceledException) { _scanCts?.Cancel(); /* Pause/Stop pressed */ }
        catch (Exception ex)
        {
            _scanCts?.Cancel();
            MessageBox.Show("Scan & Download failed: " + ex.Message, "Phoenix Leecher",
                MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        finally
        {
            if (scanTask != null)
            {
                try { await scanTask; } catch { /* already reported, or cancelled */ }
            }
            SyncRows();
            SaveEntries();
            _scanCts?.Dispose();
            _scanCts = null;
            var pipe = _pipeCts;
            if (_dlBatchCts == pipe) _dlBatchCts = null; // don't clobber a Download All batch started mid-pipeline
            pipe?.Dispose();
            _pipeCts = null;
            _isBatchRunning = false;
            // Batch summary: albums where BOTH pCloud and MediaFire failed.
            List<string> failed;
            lock (_bothFailedNames) { failed = new List<string>(_bothFailedNames); _bothFailedNames.Clear(); }
            if (failed.Count > 0)
                MessageBox.Show("These albums failed on both pCloud and MediaFire:\n- " + string.Join("\n- ", failed),
                    "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Warning);
            SetScanRunning(false);
        }
    }

    private void SetScanRunning(bool running)
    {
        _scanRunning = running;
        PageScanBtn.IsEnabled = !running;
        ScanBtn.IsEnabled = !running;
        StopBtn.IsEnabled = false; // enabled only by Whole scan below
    }

    private void Stop_Click(object sender, RoutedEventArgs e)
    {
        _scanCts?.Cancel();
        StopBtn.IsEnabled = false;
    }

    // ---------- downloads ----------
    /// <summary>Starts a download if possible. Returns true if a download was started.</summary>
    private bool StartDownloadNow(LeecherEntry entry)
    {
        if (entry.Status == EntryStatus.Downloading) return false;
        if (entry.Status == EntryStatus.NeedsPassword && string.IsNullOrEmpty(entry.Password))
        {
            MessageBox.Show("This link needs a password — enter it in the row and press Unlock.",
                "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Information);
            return false;
        }
        if (_dlCts.ContainsKey(entry.Id)) return false;
        var cts = new CancellationTokenSource();
        _dlCts[entry.Id] = cts;
        var task = _engine.DownloadAsync(entry, _tickProgress, cts.Token);
        _dlTasks[entry.Id] = task;
        // Clean up the task slot when done (ApplyTick removes the CTS on terminal state).
        _ = task.ContinueWith(_ => _dlTasks.TryRemove(entry.Id, out _), TaskScheduler.Default);
        return true;
    }

    private void StartDownload(LeecherEntry entry) => StartDownloadNow(entry);

    private void RowDownload_Click(object sender, RoutedEventArgs e)
    {
        if ((sender as FrameworkElement)?.DataContext is LeecherEntry entry)
            StartDownload(entry);
    }

    private void RowPause_Click(object sender, RoutedEventArgs e)
    {
        if ((sender as FrameworkElement)?.DataContext is LeecherEntry entry &&
            _dlCts.TryGetValue(entry.Id, out var cts))
            cts.Cancel();
    }

    private async void DownloadAll_Click(object sender, RoutedEventArgs e)
    {
        // Cancel any previous batch, then run a new one. Pause All cancels this
        // batch token too, so the loop stops instead of starting the next file.
        _dlBatchCts?.Cancel();
        _dlBatchCts?.Dispose();
        var batchCts = new CancellationTokenSource();
        _dlBatchCts = batchCts;
        _isBatchRunning = true;
        try
        {
            var list = _rows.Where(x => x.Status is EntryStatus.Ready or EntryStatus.Paused or EntryStatus.Failed).ToList();
            // v1.3.7: Download All runs up to N albums at once (N = Options ->
            // Max concurrent files, the IDM-style number the user picks). The
            // engine's file-slot pool is the same N, so N single-file albums
            // transfer N files at once; multi-file folders share the pool.
            var running = new List<Task>();
            foreach (var entry in list)
            {
                batchCts.Token.ThrowIfCancellationRequested();
                while (running.Count >= _engine.MaxConcurrentFiles)
                {
                    await Task.WhenAny(running.Concat(new[] { Task.Delay(Timeout.Infinite, batchCts.Token) }));
                    running.RemoveAll(t => t.IsCompleted);
                    batchCts.Token.ThrowIfCancellationRequested(); // Pause All while waiting for a slot
                }
                // Start on the UI thread; skip if not started (already running / needs password).
                bool started = (bool)Dispatcher.Invoke(() => StartDownloadNow(entry));
                if (!started) continue;
                if (_dlTasks.TryGetValue(entry.Id, out var task))
                    running.Add(task);
            }
            // Wait for the stragglers (or batch cancel) — no polling.
            if (running.Count > 0)
            {
                var all = Task.WhenAll(running);
                await Task.WhenAny(all, Task.Delay(Timeout.Infinite, batchCts.Token));
                _ = all.Exception; // observe: per-entry failures are already reported, never throw here
            }
            batchCts.Token.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException) { /* Pause All pressed */ }
        finally
        {
            _isBatchRunning = false;
            if (_dlBatchCts == batchCts) { _dlBatchCts = null; batchCts.Dispose(); }
            // Batch summary: albums where BOTH pCloud and MediaFire failed.
            List<string> failed;
            lock (_bothFailedNames) { failed = new List<string>(_bothFailedNames); _bothFailedNames.Clear(); }
            if (failed.Count > 0)
                MessageBox.Show("These albums failed on both pCloud and MediaFire:\n- " + string.Join("\n- ", failed),
                    "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private void PauseAll_Click(object sender, RoutedEventArgs e)
    {
        // Stop everything: the Download All batch loop plus every running file.
        _dlBatchCts?.Cancel();
        foreach (var cts in _dlCts.Values.ToList()) cts.Cancel();
    }

    private void ResumeAll_Click(object sender, RoutedEventArgs e)
    {
        foreach (var entry in _rows.Where(x => x.Status is EntryStatus.Paused or EntryStatus.Failed).ToList())
            StartDownload(entry);
    }

    private void RemoveEntry(LeecherEntry entry)
    {
        if (_dlCts.TryGetValue(entry.Id, out var cts)) cts.Cancel();
        // v1.3.5: a manual delete is remembered — the entry stays in _entries
        // as Deleted (hidden, persisted), so a later scan never re-adds it.
        // Completed already behaved this way (kept in _entries for dedup).
        // Dead entries are never listed (user: don't include Dead in scans),
        // so there is no Dead row to delete — no exception is needed here.
        entry.Status = EntryStatus.Deleted;
        entry.StatusText = "Deleted";
        // Keep _entries + _byId: late ticks from the cancelled download are
        // swallowed by the ApplyTick Deleted guard (which releases the CTS).
        _rows.Remove(entry);
        _passwords.Remove(entry.Id);
        SaveEntries();
        UpdateCounts();
    }

    private void RowDelete_Click(object sender, RoutedEventArgs e)
    {
        if ((sender as FrameworkElement)?.DataContext is LeecherEntry entry)
        {
            // v1.3.5: deleting is permanent (no Restore) — confirm first.
            if (MessageBox.Show($"Delete '{entry.DisplayName}'?\n\nIt will not appear in future scans.",
                    "Phoenix Leecher", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
                return;
            RemoveEntry(entry);
        }
    }

    private void DeleteSelected_Click(object sender, RoutedEventArgs e)
    {
        if (Grid.SelectedItem is LeecherEntry entry)
        {
            // v1.3.5: deleting is permanent (no Restore) — confirm first.
            if (MessageBox.Show($"Delete '{entry.DisplayName}'?\n\nIt will not appear in future scans.",
                    "Phoenix Leecher", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
                return;
            RemoveEntry(entry);
        }
    }

    private void RemoveCompleted_Click(object sender, RoutedEventArgs e)
    {
        // List only: the downloaded files stay on disk.
        foreach (var entry in _rows.Where(x => x.Status == EntryStatus.Completed).ToList())
            RemoveEntry(entry);
    }

    private void Grid_PreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        // Click on empty area (not on a row) -> clear the blue selection.
        DependencyObject? dep = e.OriginalSource as DependencyObject;
        while (dep != null && dep is not DataGridRow && dep is not DataGridCell
               && dep is not System.Windows.Controls.Primitives.ScrollBar && dep != Grid)
            dep = VisualTreeHelper.GetParent(dep);
        if (dep == Grid || dep == null)
            Grid.UnselectAll();
    }

    // ---------- password (session-only, never persisted) ----------
    private void PwBox_PasswordChanged(object sender, RoutedEventArgs e)
    {
        if (sender is PasswordBox pb && pb.DataContext is LeecherEntry entry)
            _passwords[entry.Id] = pb.Password;
    }

    private async void Unlock_Click(object sender, RoutedEventArgs e)
    {
        if ((sender as FrameworkElement)?.DataContext is not LeecherEntry entry) return;
        _passwords.TryGetValue(entry.Id, out var pw);
        entry.Status = EntryStatus.Resolving;
        entry.StatusText = "Unlocking…";
        try
        {
            string? url = entry.PrimaryHost == "pcloud" ? entry.PCloudShareUrl : entry.MediaFireShareUrl;
            if (url == null)
            {
                entry.Status = EntryStatus.NeedsPassword;
                entry.StatusText = "Password required";
                return;
            }
            var (fresh, _) = await Task.Run(() => _scanner.ResolveAndAddAsync(url, _entries, pw, CancellationToken.None));
            SyncRows();
            if (fresh.Status == EntryStatus.NeedsPassword)
            {
                MessageBox.Show("Wrong password.", "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Warning);
                entry.Status = EntryStatus.NeedsPassword;
                entry.StatusText = "Password required";
            }
            else
            {
                fresh.Password = pw; // session only, never persisted
            }
        }
        catch (Exception ex)
        {
            entry.Status = EntryStatus.Failed;
            entry.StatusText = "Unlock failed: " + ex.Message;
        }
    }

    // ---------- theme ----------
    private void ThemeToggle_Click(object sender, RoutedEventArgs e)
    {
        if (App.ThemeName == "Light") ApplyDark(); else ApplyLight();
    }
    private void LogBtn_Click(object sender, RoutedEventArgs e)
    {
        var w = new LogWindow { Owner = this };
        w.ShowDialog();
    }
    private void ThemeLight_Click(object sender, RoutedEventArgs e) => ApplyLight();
    private void ThemeDark_Click(object sender, RoutedEventArgs e) => ApplyDark();
    private void ApplyLight()
    {
        App.ApplyTheme("Light");
        ThemeBtnLabel.Text = "Dark";
        MenuLight.IsChecked = true; MenuDark.IsChecked = false;
    }
    private void ApplyDark()
    {
        App.ApplyTheme("Dark");
        ThemeBtnLabel.Text = "Light";
        MenuLight.IsChecked = false; MenuDark.IsChecked = true;
    }

    // ---------- misc ----------
    private void Concurrency_Click(object sender, RoutedEventArgs e)
    {
        if (sender is MenuItem mi && int.TryParse(mi.Tag?.ToString(), out int n))
        {
            _engine = new DownloadEngine(maxConcurrentFiles: n);
            foreach (var item in ((mi.Parent as MenuItem)?.Items.OfType<MenuItem>() ?? Enumerable.Empty<MenuItem>()))
                item.IsChecked = item == mi;
        }
    }

    private void OpenFolder_Click(object sender, RoutedEventArgs e)
    {
        Directory.CreateDirectory(DownloadEngine.DownloadRoot);
        Process.Start("explorer.exe", DownloadEngine.DownloadRoot);
    }

    // ---------- download folder (user spec 2026-11-08) ----------
    // Default stays Downloads\PhoenixLeecher (C:). The user picks any other
    // folder; it is remembered in settings.json and new downloads go there.
    // The user moves existing files himself in Explorer — the app moves nothing.
    private void ChangeFolder_Click(object sender, RoutedEventArgs e)
    {
        if (_dlCts.Count > 0 || _isBatchRunning)
        {
            MessageBox.Show("Pause all downloads first, then change the download folder.",
                "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }
        var dlg = new Microsoft.Win32.OpenFolderDialog
        {
            Title = "Choose download folder",
            InitialDirectory = DownloadEngine.DownloadRoot,
        };
        if (dlg.ShowDialog() != true || string.IsNullOrWhiteSpace(dlg.FolderName)) return;
        string folder = dlg.FolderName;
        if (string.Equals(folder, DownloadEngine.DownloadRoot, StringComparison.OrdinalIgnoreCase)) return;
        Directory.CreateDirectory(folder);
        AppSettings.SaveDownloadFolder(folder);
        DownloadEngine.SetDownloadRoot(folder);
        StatusFolder.Text = folder;
        MessageBox.Show(
            $"New downloads will be saved to:\n{folder}\n\nMove your existing files there yourself in Explorer — the app does not move them. " +
            "Move the whole folder contents as-is (album folders, single files and .part files) so paused downloads can resume instead of restarting.",
            "Phoenix Leecher", MessageBoxButton.OK, MessageBoxImage.Information);
    }

    private void About_Click(object sender, RoutedEventArgs e) =>
        new AboutWindow { Owner = this }.ShowDialog();

    private void Exit_Click(object sender, RoutedEventArgs e) => Close();

    // ---------- entry list persistence (user spec: list survives app restart) ----------
    // v1.3.7: SavedEntry now lives in Models/Entry.cs; EntryStore (SQLite) reads/writes it.

    private void SaveEntries()
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(EntriesPath)!);
            // Save in display order (_rows), not dict order, so the layout survives restart.
            var list = _rows.Select(e => new SavedEntry
            {
                NormalizedKey = e.NormalizedKey,
                DisplayName = e.DisplayName,
                IsFolder = e.IsFolder,
                TotalSize = e.TotalSize,
                PrimaryHost = e.PrimaryHost,
                PCloudShareUrl = e.PCloudShareUrl,
                MediaFireShareUrl = e.MediaFireShareUrl,
                Status = e.Status.ToString(),
                DownloadedBytes = e.DownloadedBytes,
                Files = e.Files.ToList(),
            }).ToList();
            // Also persist hidden Completed (in _entries but not in _rows, per user spec
            // 2026-10-17) so dedup survives restart and Whole scan doesn't re-add them.
            // v1.3.5: same for Deleted entries (manual deletes must survive restart).
            // v1.3.5: same for Dead entries (hidden, but kept so scans skip re-resolving them).
            var visibleIds = new HashSet<string>(_rows.Select(e => e.Id));
            foreach (var kv in _entries.Values)
            {
                if ((kv.Status == EntryStatus.Completed || kv.Status == EntryStatus.Deleted || kv.Status == EntryStatus.Dead) && !visibleIds.Contains(kv.Id))
                {
                    list.Add(new SavedEntry
                    {
                        NormalizedKey = kv.NormalizedKey,
                        DisplayName = kv.DisplayName,
                        IsFolder = kv.IsFolder,
                        TotalSize = kv.TotalSize,
                        PrimaryHost = kv.PrimaryHost,
                        PCloudShareUrl = kv.PCloudShareUrl,
                        MediaFireShareUrl = kv.MediaFireShareUrl,
                        Status = kv.Status.ToString(),
                        DownloadedBytes = kv.DownloadedBytes,
                        Files = kv.Files.ToList(),
                    });
                }
            }
            new EntryStore(DbPath).SaveAll(list);
        }
        catch { /* persistence must never break the app */ }
    }

    private void LoadEntries()
    {
        try
        {
            // v1.3.7: the list lives in SQLite (entries.db) now, indexed on
            // NormalizedKey. First run after the upgrade imports entries.json
            // once, then keeps it as entries.json.bak (never deleted).
            // If the DB can't be read, fall back to the JSON files.
            List<SavedEntry>? list = null;
            var store = new EntryStore(DbPath);
            try
            {
                if (store.DatabaseExists)
                {
                    list = store.LoadAll();
                }
                else if (File.Exists(EntriesPath))
                {
                    list = JsonSerializer.Deserialize<List<SavedEntry>>(File.ReadAllText(EntriesPath));
                    if (list != null)
                    {
                        store.SaveAll(list); // succeeds first — only then the JSON is retired
                        File.Move(EntriesPath, EntriesPath + ".bak", overwrite: true);
                    }
                }
            }
            catch
            {
                // DB unreadable (e.g. native lib missing) — fall back to JSON.
                if (File.Exists(EntriesPath))
                    list = JsonSerializer.Deserialize<List<SavedEntry>>(File.ReadAllText(EntriesPath));
                else if (File.Exists(EntriesPath + ".bak"))
                    list = JsonSerializer.Deserialize<List<SavedEntry>>(File.ReadAllText(EntriesPath + ".bak"));
            }
            if (list == null) return;
            var rebuilt = new List<LeecherEntry>();
            foreach (var s in list)
            {
                // Mid-resolve entries (no files yet) can't download; user re-scans.
                // Dead / NeedsPassword are link states, kept without files.
                if (s.Files.Count == 0 && s.Status != "Dead" && s.Status != "NeedsPassword" && s.Status != "Deleted")
                    continue;
                var entry = new LeecherEntry
                {
                    DisplayName = s.DisplayName,
                    NormalizedKey = s.NormalizedKey,
                    IsFolder = s.IsFolder,
                    FileCount = s.Files.Count,
                    TotalSize = s.TotalSize,
                    PrimaryHost = s.PrimaryHost,
                };
                entry.PCloudShareUrl = s.PCloudShareUrl;
                entry.MediaFireShareUrl = s.MediaFireShareUrl;
                foreach (var f in s.Files) entry.Files.Add(f);
                // An interrupted download becomes Paused (its .part stays on disk, resumes via Range).
                // Passwords are session-only -> protected links re-prompt after restart.
                entry.Status = s.Status switch
                {
                    "Downloading" => EntryStatus.Paused,
                    "Paused" => EntryStatus.Paused,
                    "Completed" => EntryStatus.Completed,
                    "Failed" => EntryStatus.Failed,
                    "Dead" => EntryStatus.Dead,
                    "NeedsPassword" => EntryStatus.NeedsPassword,
                    "Deleted" => EntryStatus.Deleted,
                    _ => EntryStatus.Ready,
                };
                entry.StatusText = entry.Status switch
                {
                    EntryStatus.Completed => "Completed",
                    EntryStatus.Failed => "Failed",
                    EntryStatus.Dead => "Link dead",
                    EntryStatus.NeedsPassword => "Password required",
                    EntryStatus.Paused => "Paused",
                    EntryStatus.Deleted => "Deleted",
                    _ => "Ready",
                };
                // User spec 2026-11-08: progress comes from json, no disk scan.
                // Legacy entries.json has no DownloadedBytes (defaults 0): fill it
                // from disk this once so existing paused progress is not lost.
                // After the first save the field exists and disk is never read again.
                entry.DownloadedBytes = s.DownloadedBytes > 0
                    ? s.DownloadedBytes
                    : ExistingPartBytes(entry);
                entry.Progress = entry.TotalSize > 0
                    ? Math.Min(100, entry.DownloadedBytes * 100.0 / entry.TotalSize) : 0;
                entry.RefreshDerived();
                rebuilt.Add(entry);
            }
            // v1.3.5 folder-vs-file (existing lists): drop Ready single-file
            // entries whose base name equals a folder entry's name — the folder
            // already has the file. Scan-time dedup in Scanner only sees freshly
            // resolved links (known links hit the URL fast-path), so without this
            // sweep the mp3 rows already saved in entries.json would stay forever.
            var folderNames = new HashSet<string>(rebuilt.Where(e => e.IsFolder)
                .Select(e => MyanmarText.NormalizeKey(e.DisplayName)));
            rebuilt.RemoveAll(e =>
            {
                if (e.IsFolder || e.Status != EntryStatus.Ready) return false;
                string k = LinkScanner.FileBaseKey(e.DisplayName);
                return k.Length > 0 && folderNames.Contains(k);
            });
            // User spec 2026-10-17: Completed entries stay in _entries/_byId for dedup
            // (Whole scan won't re-add them), but are NOT shown in _rows. This avoids
            // the Download All crash triggered by Completed entries in the visible list.
            foreach (var entry in rebuilt)
            {
                _entries[entry.NormalizedKey] = entry;
                _byId[entry.Id] = entry;
                if (entry.Status != EntryStatus.Completed && entry.Status != EntryStatus.Deleted && entry.Status != EntryStatus.Dead)
                    _rows.Add(entry);
            }
            RenumberRows();
            UpdateCounts();
        }
        catch { /* a corrupt file must never break startup */ }
    }

    private static long ExistingPartBytes(LeecherEntry entry)
    {
        try
        {
            string targetDir = DownloadEngine.DownloadRoot;
            if (entry.IsFolder) targetDir = Path.Combine(targetDir, DownloadEngine.Sanitize(entry.DisplayName));
            long sum = 0;
            foreach (var f in entry.Files)
            {
                string basePath = Path.Combine(targetDir, DownloadEngine.Sanitize(f.Name));
                if (File.Exists(basePath + ".part")) sum += new FileInfo(basePath + ".part").Length;
                else if (File.Exists(basePath)) sum += new FileInfo(basePath).Length;
            }
            return sum;
        }
        catch { return 0; }
    }

    protected override void OnClosed(EventArgs e)
    {
        _scanCts?.Cancel();
        foreach (var cts in _dlCts.Values) cts.Cancel();
        _syncTimer.Stop();
        base.OnClosed(e);
    }
}
