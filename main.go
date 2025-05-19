package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
    "os/exec"
	"strings"
	"time"
	"bytes"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Version      = "0.1.17" // overridden by build flags
	MetricPrefix = "ceph_vm_"
	Debug        = false
)

// CLI flags
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

func main() {
	cfg := parseFlags()
	if cfg.showVer {
		fmt.Println(Version)
		return
	}
	Debug = cfg.debug
	prometheus.MustRegister(NewCollector(cfg.pool))
	http.Handle("/metrics", promhttp.Handler())
	addr := fmt.Sprintf("%s:%d", cfg.ipAddress, cfg.port)
	log.Printf("Starting ceph-exporter on http://%s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

// RBD executor
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

// JSON structs

type poolStatus struct {
	Images []struct {
		Name      string `json:"name"`
		PeerSites []struct {
			Description string `json:"description"`
		} `json:"peer_sites"`
	} `json:"images"`
}

type snapshotStats struct {
	BytesPerSecond          float64 `json:"bytes_per_second"`
	BytesPerSnapshot        float64 `json:"bytes_per_snapshot"`
	LastSnapshotBytes       float64 `json:"last_snapshot_bytes"`
	LastSnapshotSyncSeconds float64 `json:"last_snapshot_sync_seconds"`
}

// Prometheus collector

type mirrorCollector struct {
	pool string

	descSnapSpeed                *prometheus.Desc
	descSnapBytesPerSnapshot     *prometheus.Desc
	descSnapLastSnapshotBytes    *prometheus.Desc
	descSnapLastSnapshotSyncSecs *prometheus.Desc
}

func NewCollector(pool string) prometheus.Collector {
	labels := []string{"pool", "image"}
	mp := MetricPrefix
    return &mirrorCollector{
		pool: pool,
		descSnapSpeed:                prometheus.NewDesc(mp+"snapshot_speed_mib_per_sec", "Snapshot sync speed (MiB/s)", labels, nil),
		descSnapBytesPerSnapshot:     prometheus.NewDesc(mp+"snapshot_bytes_per_snapshot_mib", "Bytes per snapshot (MiB)", labels, nil),
		descSnapLastSnapshotBytes:    prometheus.NewDesc(mp+"snapshot_last_snapshot_bytes_mib", "Last snapshot size transferred (MiB)", labels, nil),
		descSnapLastSnapshotSyncSecs: prometheus.NewDesc(mp+"snapshot_last_snapshot_sync_seconds", "Duration of last snapshot sync (s)", labels, nil),
    }
}

func (c *mirrorCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- c.descSnapSpeed
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
	var ps poolStatus
    if err := json.Unmarshal(raw, &ps); err != nil {
        log.Printf("decode pool status: %v", err)
        return
    }

    for _, img := range ps.Images {
		if len(img.PeerSites) == 0 {
            continue
        }
		desc := img.PeerSites[0].Description
		idx := strings.Index(desc, "{")
		if idx == -1 {
            continue
        }
		var stats snapshotStats
		if err := json.Unmarshal([]byte(desc[idx:]), &stats); err != nil {
			if Debug {
				log.Printf("decode stats for %s: %v", img.Name, err)
			}
            continue
        }
		labels := []string{c.pool, img.Name}
            speed := 0.0
		if stats.LastSnapshotSyncSeconds > 0 {
			speed = (stats.LastSnapshotBytes / stats.LastSnapshotSyncSeconds) / 1048576
            }
			ch <- prometheus.MustNewConstMetric(c.descSnapSpeed, prometheus.GaugeValue, speed, labels...)
		ch <- prometheus.MustNewConstMetric(c.descSnapBytesPerSnapshot, prometheus.GaugeValue, stats.BytesPerSnapshot/1048576, labels...)
		ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotBytes, prometheus.GaugeValue, stats.LastSnapshotBytes/1048576, labels...)
		ch <- prometheus.MustNewConstMetric(c.descSnapLastSnapshotSyncSecs, prometheus.GaugeValue, stats.LastSnapshotSyncSeconds, labels...)
        }
}
