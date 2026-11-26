package main

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"
)

func TestLayerCanBeReopenedForExport(t *testing.T) {
	data := []byte("asset")
	layer, err := makeLayer(map[string]layerEntry{
		"web/index.html": regularEntry("web/index.html", data, 0o644, 0, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The digest and the archive exporter open the same payload separately.
	for i := 0; i < 2; i++ {
		r, err := layer.Uncompressed()
		if err != nil {
			t.Fatal(err)
		}
		tr := tar.NewReader(r)
		h, err := tr.Next()
		if err != nil || h.Name != "web/index.html" {
			t.Fatalf("open %d: header %v, err %v", i, h, err)
		}
		content, err := io.ReadAll(tr)
		if err != nil || !bytes.Equal(content, data) {
			t.Fatalf("open %d: content %q, err %v", i, content, err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
