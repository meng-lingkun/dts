package validationreport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

func pdfEscape(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 32 || r > 126 {
			return '?'
		}
		return r
	}, s)
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "(", "\\(")
	s = strings.ReplaceAll(s, ")", "\\)")
	return s
}

func shorten(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n < 4 {
		return s[:n]
	}
	return s[:n-3] + "..."
}

func wrapLine(s string, width int) []string {
	if width < 20 || len(s) <= width {
		return []string{s}
	}
	out := []string{}
	for len(s) > width {
		cut := width
		if i := strings.LastIndexByte(s[:width+1], ' '); i > width/2 {
			cut = i
		}
		out = append(out, strings.TrimSpace(s[:cut]))
		s = strings.TrimSpace(s[cut:])
	}
	if s != "" {
		out = append(out, s)
	}
	return out
}

func reportLines(r Report) []string {
	a := r.Validation
	lines := []string{
		"QMigration Validation Acceptance Report",
		"",
		"Task: " + r.Task.Name + " (" + r.Task.ID + ")",
		"Mode: " + string(r.Task.Mode),
		"Terminal status: " + string(a.TerminalStatus),
		"Product version: " + r.ProductVersion,
		"Generated: " + r.GeneratedAt.UTC().Format(time.RFC3339),
		"Archive evidence SHA-256: " + a.EvidenceDigest,
		"",
		fmt.Sprintf("Summary: tables=%d chunks=%d covered=%d success=%d mismatch=%d error=%d missing=%d", a.TotalTables, a.TotalChunks, a.CoveredChunks, a.SuccessChunks, a.MismatchChunks, a.ErrorChunks, a.MissingChunks),
	}
	if a.ValidationBarrierPositionType != "" || a.ValidationBarrierPosition != "" {
		lines = append(lines, "Barrier: "+strings.TrimSpace(a.ValidationBarrierPositionType+" "+a.ValidationBarrierPosition+" "+a.ValidationBarrierResource))
	}
	lines = append(lines, "", "Table evidence:")
	tables := append([]struct{ key, line string }{}, make([]struct{ key, line string }, 0, len(a.Tables))...)
	for _, t := range a.Tables {
		src := t.SourceSchema + "." + t.SourceTable
		dst := t.TargetSchema + "." + t.TargetTable
		tables = append(tables, struct{ key, line string }{
			key:  src + "\x00" + dst,
			line: fmt.Sprintf("%s -> %s | %s | chunks %d/%d | S/M/E/X %d/%d/%d/%d | rows %d/%d | evidence %s", src, dst, t.EvidenceScope, t.CoveredChunks, t.TotalChunks, t.SuccessChunks, t.MismatchChunks, t.ErrorChunks, t.MissingChunks, t.SourceRows, t.TargetRows, shorten(t.EvidenceDigest, 24)),
		})
	}
	sort.SliceStable(tables, func(i, j int) bool { return tables[i].key < tables[j].key })
	for _, t := range tables {
		lines = append(lines, t.line)
	}
	lines = append(lines, "", "Verify this artifact against the accompanying SHA-256/HMAC manifest.")
	var wrapped []string
	for _, line := range lines {
		wrapped = append(wrapped, wrapLine(line, 96)...)
	}
	return wrapped
}

func renderPDF(r Report) ([]byte, error) {
	lines := reportLines(r)
	const perPage = 48
	pages := (len(lines) + perPage - 1) / perPage
	if pages == 0 {
		pages = 1
	}
	objects := make([][]byte, 3+pages*2)
	objects[0] = []byte("<< /Type /Catalog /Pages 2 0 R >>")
	var kids strings.Builder
	for p := 0; p < pages; p++ {
		if p > 0 {
			kids.WriteByte(' ')
		}
		fmt.Fprintf(&kids, "%d 0 R", 4+p*2)
	}
	objects[1] = []byte(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", kids.String(), pages))
	objects[2] = []byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
	for p := 0; p < pages; p++ {
		pageObj := 4 + p*2
		contentObj := pageObj + 1
		objects[pageObj-1] = []byte(fmt.Sprintf("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>", contentObj))
		start := p * perPage
		end := start + perPage
		if end > len(lines) {
			end = len(lines)
		}
		var stream strings.Builder
		stream.WriteString("BT\n/F1 9 Tf\n48 750 Td\n12 TL\n")
		for i := start; i < end; i++ {
			fmt.Fprintf(&stream, "(%s) Tj\nT*\n", pdfEscape(lines[i]))
		}
		stream.WriteString("ET\n")
		data := []byte(stream.String())
		objects[contentObj-1] = []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(data), data))
	}
	var out bytes.Buffer
	out.WriteString("%PDF-1.4\n%QMigration\n")
	offsets := make([]int, len(objects)+1)
	for i, obj := range objects {
		offsets[i+1] = out.Len()
		fmt.Fprintf(&out, "%d 0 obj\n", i+1)
		out.Write(obj)
		out.WriteString("\nendobj\n")
	}
	xref := out.Len()
	fmt.Fprintf(&out, "xref\n0 %d\n", len(objects)+1)
	out.WriteString("0000000000 65535 f \n")
	for i := 1; i <= len(objects); i++ {
		fmt.Fprintf(&out, "%010d 00000 n \n", offsets[i])
	}
	fmt.Fprintf(&out, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xref)
	return out.Bytes(), nil
}

func renderPDFWithOptionalRenderer(r Report, html []byte) ([]byte, error) {
	bin := strings.TrimSpace(os.Getenv("QMIGRATION_VALIDATION_REPORT_PDF_RENDERER"))
	if bin == "" {
		return renderPDF(r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Stdin = bytes.NewReader(html)
	var out, er bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &er
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("external PDF renderer: %w: %s", err, strings.TrimSpace(er.String()))
	}
	if out.Len() < 8 || out.Len() > 64<<20 || !bytes.HasPrefix(out.Bytes(), []byte("%PDF-")) {
		return nil, errors.New("external PDF renderer returned invalid/oversize PDF")
	}
	return out.Bytes(), nil
}
