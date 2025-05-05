package main

import (
    "fmt"
    "log"
    "net/http"
	"context"
	"encoding/json"
    "os/exec"
	"bytes"
    "strings"
	"flag"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Version      = "0.1.16" // overridden by build flags
	MetricPrefix = "ceph_vm_"
	Debug        = false
)

type mirrorCollector struct {
	pool string
	descJournalSpeed              *prometheus.Desc
	descJournalEntriesBehind      *prometheus.Desc
	descJournalEntriesPerSecond   *prometheus.Desc
	descJournalSecondsUntilSynced *prometheus.Desc
	descSnapSyncPercent           *prometheus.Desc
	descSnapSpeed                 *prometheus.Desc
	descSnapSecondsUntilSynced    *prometheus.Desc
	descSnapBytesPerSnapshot      *prometheus.Desc
	descSnapLastSnapshotBytes     *prometheus.Desc
	descSnapLastSnapshotSyncSecs  *prometheus.Desc
}

type PoolStatus struct {
    Images []struct {
    Name string `json:"name"`
        Mode string `json:"mirror_image_mode"`
    } `json:"images"`
}

type MirrorStatus struct {
    Mode            string          `json:"mode"`
	ReplayingStatus *ReplayingStats `json:"replaying_status,omitempty"`
	SnapshotStatus  *SnapshotStats  `json:"snapshot_status,omitempty"`
}

type ReplayingStats struct {
    BytesPerSecond       float64 `json:"bytes_per_second"`
    EntriesBehindPrimary float64 `json:"entries_behind_primary"`
    EntriesPerSecond     float64 `json:"entries_per_second"`
    SecondsUntilSynced   float64 `json:"seconds_until_synced"`
}

type SnapshotStats struct {
    SyncingPercent          float64 `json:"syncing_percent"`
    SecondsUntilSynced      float64 `json:"seconds_until_synced"`
    BytesPerSnapshot        float64 `json:"bytes_per_snapshot"`
    LastSnapshotBytes       float64 `json:"last_snapshot_bytes"`
    LastSnapshotSyncSeconds float64 `json:"last_snapshot_sync_seconds"`
}

func RunRBD(ctx context.Context, args ...string) ([]byte, error) {
	if Debug {
		log.Printf("[DEBUG] run: rbd %s", strings.Join(args, " "))
	}
	cmd := exec.CommandContext(ctx, "rbd", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil && Debug {
		log.Printf("[DEBUG] rbd error: %v; stderr: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, err
}

func NewCollector(pool string) prometheus.Collector {
	labels := []string{"pool", "image"}
	mp := MetricPrefix
    return &mirrorCollector{
		pool: pool,
		descJournalSpeed:              prometheus.NewDesc(mp+"journal_speed_mib_per_sec", "Journal replay speed (MiB/s)", labels, nil),
		descJournalEntriesBehind:      prometheus.NewDesc(mp+"journal_entries_behind_primary", "Journal entries behind primary", labels, nil),
		descJournalEntriesPerSecond:   prometheus.NewDesc(mp+"journal_entries_per_sec", "Journal entries applied per second", labels, nil),
		descJournalSecondsUntilSynced: prometheus.NewDesc(mp+"journal_seconds_until_synced", "Estimated seconds until fully synced", labels, nil),
		descSnapSyncPercent:           prometheus.NewDesc(mp+"snapshot_sync_percent", "Snapshot mirroring progress (%)", labels, nil),
		descSnapSpeed:                 prometheus.NewDesc(mp+"snapshot_speed_mib_per_sec", "Snapshot sync speed (MiB/s)", labels, nil),
		descSnapSecondsUntilSynced:    prometheus.NewDesc(mp+"snapshot_seconds_until_synced", "Estimated seconds until fully synced", labels, nil),
		descSnapBytesPerSnapshot:      prometheus.NewDesc(mp+"snapshot_bytes_per_snapshot_mib", "Bytes per snapshot (MiB)", labels, nil),
		descSnapLastSnapshotBytes:     prometheus.NewDesc(mp+"snapshot_last_snapshot_bytes_mib", "Size of last snapshot transferred (MiB)", labels, nil),
		descSnapLastSnapshotSyncSecs:  prometheus.NewDesc(mp+"snapshot_last_snapshot_sync_seconds", "Duration of last snapshot sync (s)", labels, nil),
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

	raw, err := RunRBD(ctx, "mirror", "pool", "status", c.pool, "--verbose", "--format", "json")
    if err != nil {
        log.Printf("mirror pool status error: %v", err)
        return
    }

	var ps PoolStatus
    if err := json.Unmarshal(raw, &ps); err != nil {
        log.Printf("decode pool status: %v", err)
        return
    }

    for _, img := range ps.Images {
        if img.Mode != "journal" && img.Mode != "snapshot" {
            continue
        }
		sraw, err := RunRBD(ctx, "mirror", "image", "status", fmt.Sprintf("%s/%s", c.pool, img.Name), "--format", "json")
        if err != nil {
            log.Printf("status for %s: %v", img.Name, err)
            continue
        }
		var ms MirrorStatus
        if err := json.Unmarshal(sraw, &ms); err != nil {
			log.Printf("decode status for %s: %v", img.Name, err)
            continue
        }
		labels := []string{c.pool, img.Name}
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
			ch <- prometheus.MustNewConstMetric(c.descSnapSpeed, prometheus.GaugeValue, speed, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapSecondsUntilSynced, prometheus.GaugeValue, ms.SnapshotStatus.SecondsUntilSynced, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapBytesPerSnapshot, prometheus.GaugeValue, ms.SnapshotStatus.BytesPerSnapshot/1048576, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotBytes, prometheus.GaugeValue, ms.SnapshotStatus.LastSnapshotBytes/1048576, labels...)
            ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotSyncSecs, prometheus.GaugeValue, ms.SnapshotStatus.LastSnapshotSyncSeconds, labels...)
        }
    }
}

func main() {
    cfg := parseFlags()

    if cfg.showVer {
		fmt.Println(Version)
        return
    }

	Debug = cfg.debug

	mc := NewCollector(cfg.pool)
	prometheus.MustRegister(mc)

	http.Handle("/metrics", promhttp.Handler())
    addr := fmt.Sprintf("%s:%d", cfg.ipAddress, cfg.port)
	log.Printf("Starting ceph-exporter on http://%s", addr)
    if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func parseFlags() (cfg struct {
	pool      string
	ipAddress string
	port      int
	showVer   bool
	debug     bool
}) {
	flag.StringVar(&cfg.pool, "pool", "ceph-pool1", "Ceph pool to scan for VM images")
	flag.StringVar(&cfg.ipAddress, "ipaddress", "", "IP address to listen on")
	flag.IntVar(&cfg.port, "port", 9125, "TCP port to listen on")
	flag.BoolVar(&cfg.showVer, "version", false, "Print version and exit")
	flag.BoolVar(&cfg.debug, "debug", false, "Enable debug logging")
	flag.Parse()
	return
}
