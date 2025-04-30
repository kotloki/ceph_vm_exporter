package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/exec"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

// -----------------------------------------------------------------------------
// Build‑time variables (override with: go build -ldflags "-X main.version=…")
// -----------------------------------------------------------------------------

var (
    version      = "0.1"
    metricPrefix = "ceph_vm_" // change on build with: -ldflags "-X main.metricPrefix=xyz_"
)

// -----------------------------------------------------------------------------
// CLI flags
// -----------------------------------------------------------------------------

type cliFlags struct {
    pool       string
    ipAddress  string
    port       int
    showVer    bool
}

func parseFlags() cliFlags {
    var cfg cliFlags

    flag.StringVar(&cfg.pool, "pool", "ceph-pool1", "Ceph pool to scan for VM images")
    flag.StringVar(&cfg.ipAddress, "ipaddress", "", "IP address to listen on (empty = all interfaces)")
    flag.IntVar(&cfg.port, "port", 9125, "TCP port to listen on (default 9125)")
    flag.BoolVar(&cfg.showVer, "version", false, "Print application version and exit")

    flag.Parse()
    return cfg
}

// -----------------------------------------------------------------------------
// Helper: run rbd and return stdout
// -----------------------------------------------------------------------------

func runRBD(ctx context.Context, cluster string, args ...string) ([]byte, error) {
    if cluster != "" {
        args = append([]string{"--cluster", cluster}, args...)
    }
    cmd := exec.CommandContext(ctx, "rbd", args...)
    return cmd.Output()
}

// -----------------------------------------------------------------------------
// Minimal JSON structs
// -----------------------------------------------------------------------------

type mirrorStatus struct {
    Mode            string          `json:"mode"`
    ReplayingStatus *replayingStats `json:"replaying_status,omitempty"`
    SnapshotStatus  *snapshotStats  `json:"snapshot_status,omitempty"`
}

type replayingStats struct {
    BytesPerSecond       float64 `json:"bytes_per_second"`
    EntriesBehindPrimary float64 `json:"entries_behind_primary"`
    EntriesPerSecond     float64 `json:"entries_per_second"`
    SecondsUntilSynced   float64 `json:"seconds_until_synced"`
}

type snapshotStats struct {
    SyncingPercent          float64 `json:"syncing_percent"`
    SecondsUntilSynced      float64 `json:"seconds_until_synced"`
    BytesPerSnapshot        float64 `json:"bytes_per_snapshot"`
    LastSnapshotBytes       float64 `json:"last_snapshot_bytes"`
    LastSnapshotSyncSeconds float64 `json:"last_snapshot_sync_seconds"`
}

// -----------------------------------------------------------------------------
// Collector — takes cluster at construction time
// -----------------------------------------------------------------------------

type mirrorCollector struct {
    cluster string
    pool    string

    descJournalSpeed               *prometheus.Desc
    descJournalEntriesBehind       *prometheus.Desc
    descJournalEntriesPerSecond    *prometheus.Desc
    descJournalSecondsUntilSynced  *prometheus.Desc

    descSnapSyncPercent            *prometheus.Desc
    descSnapSpeed                  *prometheus.Desc
    descSnapSecondsUntilSynced     *prometheus.Desc
    descSnapBytesPerSnapshot       *prometheus.Desc
    descSnapLastSnapshotBytes      *prometheus.Desc
    descSnapLastSnapshotSyncSecs   *prometheus.Desc
}

func newMirrorCollector(cluster, pool string) *mirrorCollector {
    labels := []string{"cluster", "pool", "image"}
    mp := metricPrefix

    return &mirrorCollector{
        cluster: cluster,
        pool:    pool,

        descJournalSpeed:              prometheus.NewDesc(mp+"journal_speed_mib_per_sec", "Journal replay speed (MiB/s)", labels, nil),
        descJournalEntriesBehind:      prometheus.NewDesc(mp+"journal_entries_behind_primary", "Journal entries behind primary", labels, nil),
        descJournalEntriesPerSecond:   prometheus.NewDesc(mp+"journal_entries_per_sec", "Journal entries applied per second", labels, nil),
        descJournalSecondsUntilSynced: prometheus.NewDesc(mp+"journal_seconds_until_synced", "Estimated seconds until fully synced", labels, nil),

        descSnapSyncPercent:           prometheus.NewDesc(mp+"snapshot_sync_percent", "Snapshot mirroring progress (%)", labels, nil),
        descSnapSpeed:                 prometheus.NewDesc(mp+"snapshot_speed_mib_per_sec", "Snapshot sync speed (MiB/s)", labels, nil),
        descSnapSecondsUntilSynced:    prometheus.NewDesc(mp+"snapshot_seconds_until_synced", "Estimated seconds until fully synced", labels, nil),
        descSnapBytesPerSnapshot:      prometheus.NewDesc(mp+"snapshot_bytes_per_snapshot_mib", "Bytes transferred per snapshot (MiB)", labels, nil),
        descSnapLastSnapshotBytes:     prometheus.NewDesc(mp+"snapshot_last_snapshot_bytes_mib", "Size of last snapshot transferred (MiB)", labels, nil),
        descSnapLastSnapshotSyncSecs:  prometheus.NewDesc(mp+"snapshot_last_snapshot_sync_seconds", "Duration of last snapshot sync (seconds)", labels, nil),
    }
}

func (c *mirrorCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- c.descJournalSpeed
    ch <- c.descJournalEntriesBehind
    ch <- c.descJournalEntriesPerSecond
    ch <- c.descJournalSecondsUntilSynced

    ch <- c.descSnapSyncPercent
    ch <- c.descSnapSpeed
    ch <- c.descSnapSecondsUntilSynced
    ch <- c.descSnapBytesPerSnapshot
    ch <- c.descSnapLastSnapshotBytes
    ch <- c.descSnapLastSnapshotSyncSecs
}

func (c *mirrorCollector) Collect(ch chan<- prometheus.Metric) {
    ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
    defer cancel()

    imgListRaw, err := runRBD(ctx, c.cluster, "ls", "-p", c.pool, "--format", "json")
    if err != nil {
        log.Printf("error listing images (cluster %s): %v", c.cluster, err)
        return
    }

    var images []string
    if err := json.Unmarshal(imgListRaw, &images); err != nil {
        log.Printf("error parsing image list JSON: %v", err)
        return
    }

    for _, img := range images {
        statusRaw, err := runRBD(ctx, c.cluster, "mirror", "image", "status", fmt.Sprintf("%s/%s", c.pool, img), "--format", "json")
        if err != nil {
            log.Printf("error getting status for %s: %v", img, err)
            continue
        }

        var ms mirrorStatus
        if err := json.Unmarshal(statusRaw, &ms); err != nil {
            log.Printf("error parsing JSON for %s: %v", img, err)
            continue
        }

        if ms.Mode != "journal" && ms.Mode != "snapshot" {
            continue
        }

        lbl := []string{c.cluster, c.pool, img}

        switch ms.Mode {
        case "journal":
            if ms.ReplayingStatus == nil {
                continue
            }
            ch <- prometheus.MustNewConstMetric(c.descJournalSpeed, prometheus.GaugeValue, ms.ReplayingStatus.BytesPerSecond/1048576, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descJournalEntriesBehind, prometheus.GaugeValue, ms.ReplayingStatus.EntriesBehindPrimary, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descJournalEntriesPerSecond, prometheus.GaugeValue, ms.ReplayingStatus.EntriesPerSecond, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descJournalSecondsUntilSynced, prometheus.GaugeValue, ms.ReplayingStatus.SecondsUntilSynced, lbl...)

        case "snapshot":
            if ms.SnapshotStatus == nil {
                continue
            }
            speedMib := 0.0
            if ms.SnapshotStatus.LastSnapshotSyncSeconds > 0 {
                speedMib = (ms.SnapshotStatus.LastSnapshotBytes / ms.SnapshotStatus.LastSnapshotSyncSeconds) / 1048576
            }
            ch <- prometheus.MustNewConstMetric(c.descSnapSyncPercent, prometheus.GaugeValue, ms.SnapshotStatus.SyncingPercent, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descSnapSpeed, prometheus.GaugeValue, speedMib, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descSnapSecondsUntilSynced, prometheus.GaugeValue, ms.SnapshotStatus.SecondsUntilSynced, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descSnapBytesPerSnapshot, prometheus.GaugeValue, ms.SnapshotStatus.BytesPerSnapshot/1048576, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotBytes, prometheus.GaugeValue, ms.SnapshotStatus.LastSnapshotBytes/1048576, lbl...)
            ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotSyncSecs, prometheus.GaugeValue, ms.SnapshotStatus.LastSnapshotSyncSeconds, lbl...)
        }
    }
}

// -----------------------------------------------------------------------------
// HTTP handler that builds collector on demand
// -----------------------------------------------------------------------------

func metricsHandler(pool string) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        cluster := r.URL.Query().Get("cluster")
        // Build per‑request registry so we can inject cluster param
        reg := prometheus.NewRegistry()
        reg.MustRegister(newMirrorCollector(cluster, pool))

        promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
    }
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

func main() {
    cfg := parseFlags()

    if cfg.showVer {
        fmt.Println(version)
        os.Exit(0)
    }

    http.HandleFunc("/metrics", metricsHandler(cfg.pool))

    addr := fmt.Sprintf("%s:%d", cfg.ipAddress, cfg.port)
    log.Printf("Ceph VM mirror exporter %s listening on %s (pool=%q)", version, addr, cfg.pool)

    if err := http.ListenAndServe(addr, nil); err != nil {
        log.Fatalf("server error: %v", err)
    }
}
