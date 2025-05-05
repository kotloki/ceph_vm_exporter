package main

import (
    "bytes"
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os/exec"
    "strings"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

//------------------------------------------------------------------------
// Build‑time variables (go build -ldflags "-X main.version=…")
//------------------------------------------------------------------------

var (
    version      = "0.1.15"
    metricPrefix = "ceph_vm_"
    debug        bool
)

// ---------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------

type cliFlags struct {
    pool      string
    ipAddress string
    port      int
    showVer   bool
    debug     bool
}

func parseFlags() cliFlags {
    var c cliFlags
    flag.StringVar(&c.pool, "pool", "ceph-pool1", "Ceph pool to scan for VM images")
    flag.StringVar(&c.ipAddress, "ipaddress", "", "IP address to listen on (empty = all interfaces)")
    flag.IntVar(&c.port, "port", 9125, "TCP port to listen on (default 9125)")
    flag.BoolVar(&c.showVer, "version", false, "Print version and exit")
    flag.BoolVar(&c.debug, "debug", false, "Enable verbose debug logging")
    flag.Parse()
    return c
}

// ---------------------------------------------------------------
// rbd wrapper with debug
// ---------------------------------------------------------------

func runRBD(ctx context.Context, cluster string, args ...string) ([]byte, error) {
    if cluster != "" {
        args = append([]string{"--cluster", cluster}, args...)
    }
    if debug {
        log.Printf("[DEBUG] run: rbd %s", strings.Join(args, " "))
    }
    cmd := exec.CommandContext(ctx, "rbd", args...)
    var stderr bytes.Buffer
    cmd.Stderr = &stderr
    out, err := cmd.Output()
    if err != nil && debug {
        log.Printf("[DEBUG] rbd error: %v; stderr: %s", err, strings.TrimSpace(stderr.String()))
    }
    return out, err
}

// ---------------------------------------------------------------
// JSON structs
// ---------------------------------------------------------------

type poolStatus struct {
    Images []struct {
    Name string `json:"name"`
        Mode string `json:"mirror_image_mode"`
    } `json:"images"`
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

// ---------------------------------------------------------------
// Prometheus collector
// ---------------------------------------------------------------

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

func newCollector(cluster, pool string) *mirrorCollector {
    lbl := []string{"cluster", "pool", "image"}
    mp := metricPrefix
    return &mirrorCollector{
        cluster: cluster,
        pool:    pool,
        descJournalSpeed:              prometheus.NewDesc(mp+"journal_speed_mib_per_sec", "Journal replay speed (MiB/s)", lbl, nil),
        descJournalEntriesBehind:      prometheus.NewDesc(mp+"journal_entries_behind_primary", "Journal entries behind primary", lbl, nil),
        descJournalEntriesPerSecond:   prometheus.NewDesc(mp+"journal_entries_per_sec", "Journal entries applied per second", lbl, nil),
        descJournalSecondsUntilSynced: prometheus.NewDesc(mp+"journal_seconds_until_synced", "Estimated seconds until fully synced", lbl, nil),
        descSnapSyncPercent:           prometheus.NewDesc(mp+"snapshot_sync_percent", "Snapshot mirroring progress (%)", lbl, nil),
        descSnapSpeed:                 prometheus.NewDesc(mp+"snapshot_speed_mib_per_sec", "Snapshot sync speed (MiB/s)", lbl, nil),
        descSnapSecondsUntilSynced:    prometheus.NewDesc(mp+"snapshot_seconds_until_synced", "Estimated seconds until fully synced", lbl, nil),
        descSnapBytesPerSnapshot:      prometheus.NewDesc(mp+"snapshot_bytes_per_snapshot_mib", "Bytes per snapshot (MiB)", lbl, nil),
        descSnapLastSnapshotBytes:     prometheus.NewDesc(mp+"snapshot_last_snapshot_bytes_mib", "Last snapshot size (MiB)", lbl, nil),
        descSnapLastSnapshotSyncSecs:  prometheus.NewDesc(mp+"snapshot_last_snapshot_sync_seconds", "Last snapshot sync time (s)", lbl, nil),
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
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()

    // 1. mirror pool status
    raw, err := runRBD(ctx, c.cluster, "mirror", "pool", "status", c.pool, "--verbose", "--format", "json")
    if err != nil {
        log.Printf("mirror pool status error: %v", err)
        return
    }

    var ps poolStatus
    if err := json.Unmarshal(raw, &ps); err != nil {
        log.Printf("decode pool status: %v", err)
        return
    }

    for _, img := range ps.Images {
        if img.Mode != "journal" && img.Mode != "snapshot" {
            continue
        }

        sraw, err := runRBD(ctx, c.cluster, "mirror", "image", "status", fmt.Sprintf("%s/%s", c.pool, img.Name), "--format", "json")
        if err != nil {
            log.Printf("status for %s: %v", img.Name, err)
            continue
        }
        var ms mirrorStatus
        if err := json.Unmarshal(sraw, &ms); err != nil {
            log.Printf("decode status: %v", err)
            continue
        }
        lbl := []string{c.cluster, c.pool, img.Name}
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
            speed := 0.0
            if ms.SnapshotStatus.LastSnapshotSyncSeconds > 0 {
                speed = (ms.SnapshotStatus.LastSnapshotBytes / ms.SnapshotStatus.LastSnapshotSyncSeconds) / 1048576
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

    debug = cfg.debug            // debug enabling

    if cfg.showVer {
        fmt.Println(version)
        return
    }

    http.HandleFunc("/metrics", metricsHandler(cfg.pool))

    addr := fmt.Sprintf("%s:%d", cfg.ipAddress, cfg.port)
    log.Printf("Ceph VM mirror exporter %s listening on %s (pool=%q)", version, addr, cfg.pool)

    if err := http.ListenAndServe(addr, nil); err != nil {
        log.Fatalf("server error: %v", err)
    }
}
