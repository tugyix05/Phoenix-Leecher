using System.IO;
using System.Text.Json;

namespace PhoenixLeecher.Services;

/// <summary>
/// App settings (user spec 2026-11-08): the download folder.
/// Default stays %UserProfile%\Downloads\PhoenixLeecher (C:).
/// The user picks any other folder; it is remembered here in
/// %AppData%\PhoenixLeecher\settings.json and survives restart.
/// The user moves existing files himself in Explorer; the app
/// never moves files and never scans the disk to find them.
/// Paths are built from the chosen root + the exact names in
/// entries.json, and progress comes from DownloadedBytes saved
/// in entries.json (see MainWindow SavedEntry).
/// </summary>
public static class AppSettings
{
    private sealed class SettingsFile
    {
        public string? DownloadFolder { get; set; }
    }

    private static string SettingsPath => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "PhoenixLeecher", "settings.json");

    public static string? LoadDownloadFolder()
    {
        try
        {
            if (!File.Exists(SettingsPath)) return null;
            var s = JsonSerializer.Deserialize<SettingsFile>(File.ReadAllText(SettingsPath));
            return string.IsNullOrWhiteSpace(s?.DownloadFolder) ? null : s!.DownloadFolder;
        }
        catch { return null; }
    }

    public static void SaveDownloadFolder(string folder)
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(SettingsPath)!);
            File.WriteAllText(SettingsPath, JsonSerializer.Serialize(
                new SettingsFile { DownloadFolder = folder },
                new JsonSerializerOptions { WriteIndented = true }));
        }
        catch { /* settings must never break the app */ }
    }
}
