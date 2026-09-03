package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

type layerEntry struct {
	header tar.Header
	data   []byte
}

func main() {
	var binaryDir, webDir, nginxConfig, outputDir, version, baseWebArchive string
	var serverOnly bool
	flag.StringVar(&binaryDir, "binary-dir", "", "directory containing linux/amd64 QMigration binaries")
	flag.StringVar(&webDir, "web-dir", "", "built web dist directory")
	flag.StringVar(&nginxConfig, "nginx-config", "", "nginx default.conf")
	flag.StringVar(&outputDir, "output-dir", "", "output directory for Docker image archives")
	flag.StringVar(&version, "version", "", "QMigration version")
	flag.StringVar(&baseWebArchive, "base-web-archive", "", "existing web Docker archive to refresh without a registry")
	flag.BoolVar(&serverOnly, "server-only", false, "build only the server image archive")
	flag.Parse()
	required := map[string]string{"output-dir": outputDir, "version": version}
	if baseWebArchive == "" {
		required["binary-dir"] = binaryDir
	}
	if !serverOnly {
		required["web-dir"] = webDir
		required["nginx-config"] = nginxConfig
	}
	for label, value := range required {
		if strings.TrimSpace(value) == "" {
			fatalf("--%s is required", label)
		}
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		fatalf("create output directory: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	platform := v1.Platform{OS: "linux", Architecture: "amd64"}
	if baseWebArchive != "" {
		base, err := tarball.ImageFromPath(baseWebArchive, nil)
		if err != nil {
			fatalf("read base web archive: %v", err)
		}
		web, err := appendWebLayer(base, webDir, nginxConfig)
		if err != nil {
			fatalf("refresh web image: %v", err)
		}
		if err := writeImage(filepath.Join(outputDir, "qmigration-web-"+version+".tar"), "qmigration/web:"+version, web); err != nil {
			fatalf("write web image: %v", err)
		}
		return
	}

	server, err := buildServer(ctx, platform, binaryDir)
	if err != nil {
		fatalf("build server image: %v", err)
	}
	if err := writeImage(filepath.Join(outputDir, "qmigration-server-"+version+".tar"), "qmigration/server:"+version, server); err != nil {
		fatalf("write server image: %v", err)
	}
	if serverOnly {
		return
	}

	web, err := buildWeb(ctx, platform, webDir, nginxConfig)
	if err != nil {
		fatalf("build web image: %v", err)
	}
	if err := writeImage(filepath.Join(outputDir, "qmigration-web-"+version+".tar"), "qmigration/web:"+version, web); err != nil {
		fatalf("write web image: %v", err)
	}

	postgres, err := pullImage(ctx, "postgres:17", platform)
	if err != nil {
		fatalf("pull postgres image: %v", err)
	}
	if err := writeImage(filepath.Join(outputDir, "postgres-17.tar"), "qmigration/postgres:17", postgres); err != nil {
		fatalf("write postgres image: %v", err)
	}
}

func pullImage(ctx context.Context, ref string, platform v1.Platform) (v1.Image, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return nil, err
	}
	fmt.Printf("pulling %s for %s/%s\n", ref, platform.OS, platform.Architecture)
	return remote.Image(parsed, remote.WithContext(ctx), remote.WithPlatform(platform), remote.WithTransport(retryTransport{base: http.DefaultTransport}))
}

type retryTransport struct{ base http.RoundTripper }

func (t retryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt*attempt) * time.Second
			select {
			case <-request.Context().Done():
				return nil, request.Context().Err()
			case <-time.After(delay):
			}
		}
		response, err := t.base.RoundTrip(request.Clone(request.Context()))
		if err == nil && response.StatusCode < 500 && response.StatusCode != http.StatusTooManyRequests {
			return response, nil
		}
		if response != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
			_ = response.Body.Close()
			lastErr = fmt.Errorf("temporary HTTP status %s", response.Status)
		} else {
			lastErr = err
		}
		fmt.Printf("retrying %s after attempt %d: %v\n", request.URL.Host, attempt+1, lastErr)
	}
	return nil, lastErr
}

func buildServer(ctx context.Context, platform v1.Platform, binaryDir string) (v1.Image, error) {
	base, err := pullImage(ctx, "alpine:3.22", platform)
	if err != nil {
		return nil, err
	}
	entries := map[string]layerEntry{}
	required := []string{
		"qmigration-server", "qmigration-worker", "qmigrationctl", "qmigration-cdc-bridge", "qmigration-binlog-inspect",
		"qmigration-mysql-cdc", "qmigration-tidb-cdc", "qmigration-postgres-cdc", "qmigration-opengauss-cdc",
		"qmigration-gaussdb-cdc", "qmigration-sqlserver-cdc", "qmigration-oracle-cdc", "qmigration-db2-cdc",
		"qmigration-dameng-cdc", "qmigration-gbase-cdc", "qmigration-gbase8s-cdc",
	}
	for _, binary := range required {
		data, readErr := os.ReadFile(filepath.Join(binaryDir, binary))
		if readErr != nil {
			return nil, fmt.Errorf("read %s: %w", binary, readErr)
		}
		entries["usr/local/bin/"+binary] = regularEntry("usr/local/bin/"+binary, data, 0o755, 0, 0)
	}
	entries["app/"] = directoryEntry("app/", 0o755, 10001, 10001)
	entries["var/lib/qmigration/"] = directoryEntry("var/lib/qmigration/", 0o755, 10001, 10001)
	entries["var/lib/qmigration/cdc-spool/"] = directoryEntry("var/lib/qmigration/cdc-spool/", 0o755, 10001, 10001)

	if err := addAlpinePackages(ctx, entries, []string{"ca-certificates-bundle", "zstd", "zstd-libs", "libgcc", "libstdc++"}); err != nil {
		return nil, err
	}
	layer, err := makeLayer(entries)
	if err != nil {
		return nil, err
	}
	image, err := mutate.AppendLayers(base, layer)
	if err != nil {
		return nil, err
	}
	config, err := image.ConfigFile()
	if err != nil {
		return nil, err
	}
	config.Config.User = "10001:10001"
	config.Config.WorkingDir = "/app"
	config.Config.Entrypoint = []string{"/usr/local/bin/qmigration-server"}
	config.Config.Cmd = nil
	config.Config.Labels = copyMap(config.Config.Labels)
	config.Config.Labels["org.opencontainers.image.title"] = "QMigration Server and Worker"
	return mutate.ConfigFile(image, config)
}

func buildWeb(ctx context.Context, platform v1.Platform, webDir, nginxConfig string) (v1.Image, error) {
	base, err := pullImage(ctx, "nginxinc/nginx-unprivileged:1.29-alpine", platform)
	if err != nil {
		return nil, err
	}
	return appendWebLayer(base, webDir, nginxConfig)
}

func appendWebLayer(base v1.Image, webDir, nginxConfig string) (v1.Image, error) {
	entries := map[string]layerEntry{}
	configData, err := os.ReadFile(nginxConfig)
	if err != nil {
		return nil, err
	}
	entries["etc/nginx/conf.d/default.conf"] = regularEntry("etc/nginx/conf.d/default.conf", configData, 0o644, 0, 0)
	err = filepath.Walk(webDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(webDir, path)
		if relErr != nil || rel == "." {
			return relErr
		}
		name := "usr/share/nginx/html/" + filepath.ToSlash(rel)
		if info.IsDir() {
			entries[name+"/"] = directoryEntry(name+"/", 0o755, 0, 0)
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		entries[name] = regularEntry(name, data, 0o644, 0, 0)
		return nil
	})
	if err != nil {
		return nil, err
	}
	layer, err := makeLayer(entries)
	if err != nil {
		return nil, err
	}
	return mutate.AppendLayers(base, layer)
}

func addAlpinePackages(ctx context.Context, entries map[string]layerEntry, packages []string) error {
	const baseURL = "https://dl-cdn.alpinelinux.org/alpine/v3.22/main/x86_64/"
	indexBytes, err := download(ctx, baseURL+"APKINDEX.tar.gz")
	if err != nil {
		return fmt.Errorf("download Alpine index: %w", err)
	}
	index, err := readFirstTarFile(indexBytes, "APKINDEX")
	if err != nil {
		return err
	}
	versions := map[string]string{}
	for _, record := range strings.Split(string(index), "\n\n") {
		fields := map[string]string{}
		for _, line := range strings.Split(record, "\n") {
			if len(line) > 2 && line[1] == ':' {
				fields[line[:1]] = line[2:]
			}
		}
		if fields["P"] != "" && fields["V"] != "" {
			versions[fields["P"]] = fields["V"]
		}
	}
	for _, pkg := range packages {
		version := versions[pkg]
		if version == "" {
			return fmt.Errorf("Alpine package %s not found", pkg)
		}
		fmt.Printf("including Alpine package %s-%s\n", pkg, version)
		apk, err := download(ctx, baseURL+pkg+"-"+version+".apk")
		if err != nil {
			return err
		}
		if err := extractAPK(apk, entries); err != nil {
			return fmt.Errorf("extract %s: %w", pkg, err)
		}
	}
	return nil
}

func extractAPK(apk []byte, entries map[string]layerEntry) error {
	reader := bufio.NewReader(bytes.NewReader(apk))
	for {
		if _, err := reader.Peek(1); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return err
		}
		gz.Multistream(false)
		tr := tar.NewReader(gz)
		for {
			header, nextErr := tr.Next()
			if errors.Is(nextErr, io.EOF) {
				break
			}
			if nextErr != nil {
				return nextErr
			}
			name := strings.TrimPrefix(filepath.ToSlash(header.Name), "./")
			if name == ".PKGINFO" || strings.HasPrefix(name, ".SIGN.") || name == "" {
				continue
			}
			data, readErr := io.ReadAll(tr)
			if readErr != nil {
				return readErr
			}
			copyHeader := *header
			copyHeader.Name = name
			copyHeader.ModTime = time.Unix(0, 0)
			copyHeader.AccessTime = time.Time{}
			copyHeader.ChangeTime = time.Time{}
			entries[name] = layerEntry{header: copyHeader, data: data}
		}
		_, _ = io.Copy(io.Discard, gz)
		_ = gz.Close()
	}
}

func regularEntry(name string, data []byte, mode int64, uid, gid int) layerEntry {
	return layerEntry{header: tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: mode, Size: int64(len(data)), Uid: uid, Gid: gid, ModTime: time.Unix(0, 0)}, data: data}
}

func directoryEntry(name string, mode int64, uid, gid int) layerEntry {
	return layerEntry{header: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: mode, Uid: uid, Gid: gid, ModTime: time.Unix(0, 0)}}
}

func makeLayer(entries map[string]layerEntry) (v1.Layer, error) {
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := entries[name]
		if err := tw.WriteHeader(&entry.header); err != nil {
			return nil, err
		}
		if len(entry.data) > 0 {
			if _, err := tw.Write(entry.data); err != nil {
				return nil, err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return tarball.LayerFromReader(bytes.NewReader(data.Bytes()))
}

func readFirstTarFile(compressed []byte, wanted string) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s not found in archive", wanted)
		}
		if err != nil {
			return nil, err
		}
		if strings.TrimPrefix(header.Name, "./") == wanted {
			return io.ReadAll(tr)
		}
	}
}

func download(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: retryTransport{base: http.DefaultTransport}}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, response.Status)
	}
	return io.ReadAll(response.Body)
}

func writeImage(path, tag string, image v1.Image) error {
	parsed, err := name.NewTag(tag)
	if err != nil {
		return err
	}
	digest, err := image.Digest()
	if err != nil {
		return err
	}
	fmt.Printf("writing %s (%s) to %s\n", tag, digest, path)
	return tarball.WriteToFile(path, parsed, image)
}

func copyMap(source map[string]string) map[string]string {
	destination := make(map[string]string, len(source)+1)
	for key, value := range source {
		destination[key] = value
	}
	return destination
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", args...)
	os.Exit(1)
}
