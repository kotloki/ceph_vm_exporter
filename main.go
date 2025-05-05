package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os/exec"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

//--------------------------------------------------------------------
// Build‑time variables (override via: go build -ldflags "-X main.version=…")
//--------------------------------------------------------------------

var (
    version      = "0.1.7"
    metricPrefix = "ceph_vm_" // переопределяйте на сборке, если нужно
)

//--------------------------------------------------------------------
// CLI flags
//--------------------------------------------------------------------

type cliFlags struct {
    pool      string
    ipAddress string
    port      int
    showVer   bool
}

func parseFlags() cliFlags {
    var c cliFlags

    flag.StringVar(&c.pool, "pool", "ceph-pool1", "Ceph pool to scan for VM images")
    flag.StringVar(&c.ipAddress, "ipaddress", "", "IP address to listen on (empty = all interfaces)")
    flag.IntVar(&c.port, "port", 9125, "TCP port to listen on (default 9125)")
    flag.BoolVar(&c.showVer, "version", false, "Print version and exit")

    flag.Parse()
    return c
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

//--------------------------------------------------------------------
// JSON models
//--------------------------------------------------------------------

type imageEntry struct {
    Name string `json:"name"`
    Mode string `json:"mode"` // journal / snapshot / disabled / …
}

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

//--------------------------------------------------------------------
// Prometheus collector
//--------------------------------------------------------------------

type mirrorCollector struct {
    cluster string
    pool    string

    descJournalSpeed              *prometheus.Desc
    descJournalEntriesBehind      *prometheus.Desc
    descJournalEntriesPerSecond   *prometheus.Desc
    descJournalSecondsUntilSynced *prometheus.Desc

    descSnapSyncPercent          *prometheus.Desc
    descSnapSpeed                *prometheus.Desc
    descSnapSecondsUntilSynced   *prometheus.Desc
    descSnapBytesPerSnapshot     *prometheus.Desc
    descSnapLastSnapshotBytes    *prometheus.Desc
    descSnapLastSnapshotSyncSecs *prometheus.Desc
}

func newMirrorCollector(cluster, pool string) *mirrorCollector {
    lbls := []string{"cluster", "pool", "image"}
    mp := metricPrefix

    return &mirrorCollector{
        cluster: cluster,
        pool:    pool,

        descJournalSpeed:              prometheus.NewDesc(mp+"journal_speed_mib_per_sec", "Journal replay speed (MiB/s)", lbls, nil),
        descJournalEntriesBehind:      prometheus.NewDesc(mp+"journal_entries_behind_primary", "Journal entries behind primary", lbls, nil),
        descJournalEntriesPerSecond:   prometheus.NewDesc(mp+"journal_entries_per_sec", "Journal entries applied per second", lbls, nil),
        descJournalSecondsUntilSynced: prometheus.NewDesc(mp+"journal_seconds_until_synced", "Estimated seconds until fully synced", lbls, nil),

        descSnapSyncPercent:           prometheus.NewDesc(mp+"snapshot_sync_percent", "Snapshot mirroring progress (%)", lbls, nil),
        descSnapSpeed:                 prometheus.NewDesc(mp+"snapshot_speed_mib_per_sec", "Snapshot sync speed (MiB/s)", lbls, nil),
        descSnapSecondsUntilSynced:    prometheus.NewDesc(mp+"snapshot_seconds_until_synced", "Estimated seconds until fully synced", lbls, nil),
        descSnapBytesPerSnapshot:      prometheus.NewDesc(mp+"snapshot_bytes_per_snapshot_mib", "Bytes transferred per snapshot (MiB)", lbls, nil),
        descSnapLastSnapshotBytes:     prometheus.NewDesc(mp+"snapshot_last_snapshot_bytes_mib", "Size of last snapshot transferred (MiB)", lbls, nil),
        descSnapLastSnapshotSyncSecs:  prometheus.NewDesc(mp+"snapshot_last_snapshot_sync_seconds", "Duration of last snapshot sync (seconds)", lbls, nil),
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

    // 1. List mirring images
    listRaw, err := runRBD(ctx, c.cluster, "mirror", "image", "list", c.pool, "--format", "json")
    if err != nil {
        log.Printf("mirror image list error (cluster=%s pool=%s): %v", c.cluster, c.pool, err)
        return
    }

    var images []imageEntry
    if err := json.Unmarshal(listRaw, &images); err != nil {
        log.Printf("json decode list: %v", err)
        return
    }

    // 2. Detailed status for journal/snapshot mode images
    for _, img := range images {
        if img.Mode != "journal" && img.Mode != "snapshot" {
            continue // skip disabled, image, etc.
        }

        statusRaw, err := runRBD(ctx, c.cluster, "mirror", "image", "status", fmt.Sprintf("%s/%s", c.pool, img.Name), "--format", "json")
        if err != nil {
            log.Printf("status for %s: %v", img.Name, err)
            continue
        }

        var ms mirrorStatus
        if err := json.Unmarshal(statusRaw, &ms); err != nil {
            log.Printf("decode status for %s: %v", img.Name, err)
            continue
        }

        labels := []string{c.cluster, c.pool, img.Name}

        switch ms.Mode {
        case "journal":
            if ms.ReplayingStatus == nil {
                continue
            }
            ch <- prometheus.MustNewConstMetric(c.descJournalSpeed, prometheus.GaugeValue, ms.ReplayingStatus.BytesPerSecond/1048576, labels...)
            ch <- prometheus.MustNewConstMetric(c.descJournalEntriesBehind, prometheus.GaugeValue, ms.ReplayingStatus.EntriesBehindPrimary, labels...)
            ch <- prometheus.MustNewConstMetric(c.descJournalEntriesPerSecond, prometheus.GaugeValue, ms.ReplayingStatus.EntriesPerSecond, labels...)
            ch <- prometheus.MustNewConstMetric(c.descJournalSecondsUntilSynced, prometheus.GaugeValue, ms.ReplayingStatus.SecondsUntilSynced, labels...)

        case "snapshot":
            if ms.SnapshotStatus == nil {
                continue
            }
            speedMiB := 0.0
            if ms.SnapshotStatus.LastSnapshotSyncSeconds > 0 {
                speedMiB = (ms.SnapshotStatus.LastSnapshotBytes / ms.SnapshotStatus.LastSnapshotSyncSeconds) / 1048576
            }
            ch <- prometheus.MustNewConstMetric(c.descSnapSyncPercent, prometheus.GaugeValue, ms.SnapshotStatus.SyncingPercent, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapSpeed, prometheus.GaugeValue, speedMiB, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapSecondsUntilSynced, prometheus.GaugeValue, ms.SnapshotStatus.SecondsUntilSynced, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapBytesPerSnapshot, prometheus.GaugeValue, ms.SnapshotStatus.BytesPerSnapshot/1048576, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotBytes, prometheus.GaugeValue, ms.SnapshotStatus.LastSnapshotBytes/1048576, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotSyncSecs, prometheus.GaugeValue, ms.SnapshotStatus.LastSnapshotSyncSeconds, labels...)
        }
    }
}

//--------------------------------------------------------------------
// HTTP handler — set cluster
//--------------------------------------------------------------------

func metricsHandler(pool string) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        cluster := r.URL.Query().Get("cluster")
        // Build per‑request registry so we can inject cluster param
        reg := prometheus.NewRegistry()
        reg.MustRegister(newMirrorCollector(cluster, pool))

        promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(w, r)
    }
}

//--------------------------------------------------------------------
// main
//--------------------------------------------------------------------

func main() {
    cfg := parseFlags()

    if cfg.showVer {
        fmt.Println(version)
        return
    }
    

    http.HandleFunc("/metrics", metricsHandler(cfg.pool))

    addr := fmt.Sprintf("%s:%d", cfg.ipAddress, cfg.port)
    log.Printf("Ceph VM mirror exporter %s listening on %s (pool=\"%s\")", version, addr, cfg.pool)

    if err := http.ListenAndServe(addr, nil); err != nil {
        log.Fatalf("server error: %v", err)
    }
}
