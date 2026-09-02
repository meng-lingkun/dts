package db2log

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"qmigration/backend/internal/cdc/orderedmerge"
	cdcruntime "qmigration/backend/internal/cdc/runtime"
	"qmigration/backend/internal/domain"
	"sort"
	"strings"
	"sync"
	"time"
)

type PureScaleStream struct {
	ClusterID              string `json:"cluster_id"`
	CaptureLineage         string `json:"capture_lineage"`
	StreamID               string `json:"stream_id"`
	MemberID               string `json:"member_id"`
	Epoch                  uint64 `json:"epoch"`
	PredecessorEpoch       uint64 `json:"predecessor_epoch,omitempty"`
	ResolvedGlobalSequence uint64 `json:"resolved_global_sequence"`
	CurrentLRI             LRI    `json:"current_lri"`
}
type PureScaleStreamsResponse struct {
	Streams []PureScaleStream `json:"streams"`
}
type PureScaleFragment struct {
	StreamID          string            `json:"stream_id"`
	Epoch             uint64            `json:"epoch"`
	GlobalSequence    uint64            `json:"global_sequence"`
	TID               string            `json:"tid"`
	NextLRI           LRI               `json:"next_lri"`
	CompleteRowImages bool              `json:"complete_row_images"`
	Events            []domain.CDCEvent `json:"events"`
	SourceTimestampMS int64             `json:"source_timestamp_ms,omitempty"`
}
type PureScaleReadResponse struct {
	Stream    PureScaleStream     `json:"stream"`
	Fragments []PureScaleFragment `json:"fragments"`
}
type pureScaleVectorStream struct {
	Epoch uint64 `json:"epoch"`
	LRI   string `json:"lri"`
}
type PureScaleVector struct {
	ClusterID      string                           `json:"cluster_id"`
	CaptureLineage string                           `json:"capture_lineage"`
	GlobalSequence uint64                           `json:"global_sequence"`
	Streams        map[string]pureScaleVectorStream `json:"streams"`
}

func (v PureScaleVector) String() string { b, _ := json.Marshal(v); return string(b) }
func ParsePureScaleVector(raw string) (PureScaleVector, error) {
	var v PureScaleVector
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return v, err
	}
	if v.ClusterID == "" || !validLineage(v.CaptureLineage) || len(v.Streams) == 0 {
		return v, errors.New("invalid DB2_PURESCALE_VECTOR identity")
	}
	return v, nil
}
func validLineage(s string) bool {
	b, err := hex.DecodeString(strings.TrimSpace(s))
	return err == nil && len(b) == 32
}

func (c *Client) PureScaleStreams(ctx context.Context) (*PureScaleStreamsResponse, error) {
	var out PureScaleStreamsResponse
	if err := c.do(ctx, http.MethodGet, "/v1/streams", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func (c *Client) PureScaleRead(ctx context.Context, stream string, start LRI, maxRecords, maxBytes int) (*PureScaleReadResponse, error) {
	if stream == "" {
		return nil, errors.New("DB2 pureScale stream is required")
	}
	if maxRecords <= 0 {
		maxRecords = 4096
	}
	if maxBytes <= 0 {
		maxBytes = 32 << 20
	}
	p := fmt.Sprintf("/v1/streams/%s/records?start_lri=%s&max_records=%d&max_bytes=%d", url.PathEscape(stream), url.QueryEscape(start.String()), maxRecords, maxBytes)
	var out PureScaleReadResponse
	if err := c.do(ctx, http.MethodGet, p, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type PureScaleReader struct {
	mu         sync.Mutex
	agent      *Client
	selections map[string]Selection
	vector     PureScaleVector
	ready      []*cdcruntime.Transaction
	resource   string
}

func NewPureScaleReader(ctx context.Context, agent *Client, start *PureScaleVector, selections []Selection, resource string) (*PureScaleReader, error) {
	if agent == nil {
		return nil, errors.New("nil DB2 pureScale agent")
	}
	sr, err := agent.PureScaleStreams(ctx)
	if err != nil {
		return nil, err
	}
	if len(sr.Streams) < 2 {
		return nil, errors.New("DB2 pureScale requires at least two streams")
	}
	sel := map[string]Selection{}
	for _, s := range selections {
		sel[strings.ToLower(s.Schema+"."+s.Table)] = s
	}
	if len(sel) == 0 {
		return nil, errors.New("DB2 pureScale requires selected tables")
	}
	v := PureScaleVector{Streams: map[string]pureScaleVectorStream{}}
	if start != nil {
		v = *start
		if v.Streams == nil {
			v.Streams = map[string]pureScaleVectorStream{}
		}
	}
	cluster, lineage := "", ""
	seen := map[string]bool{}
	for _, st := range sr.Streams {
		if err := validatePureScaleStream(st, cluster, lineage); err != nil {
			return nil, err
		}
		if cluster == "" {
			cluster = st.ClusterID
			lineage = st.CaptureLineage
		}
		if seen[st.StreamID] {
			return nil, fmt.Errorf("duplicate DB2 pureScale stream %s", st.StreamID)
		}
		seen[st.StreamID] = true
		old, ok := v.Streams[st.StreamID]
		if ok && old.Epoch != 0 && st.Epoch != old.Epoch && st.PredecessorEpoch != old.Epoch {
			return nil, fmt.Errorf("DB2 pureScale stream %s epoch %d does not prove predecessor %d", st.StreamID, st.Epoch, old.Epoch)
		}
		if !ok {
			v.Streams[st.StreamID] = pureScaleVectorStream{Epoch: st.Epoch, LRI: st.CurrentLRI.String()}
		}
	}
	if v.ClusterID != "" && (v.ClusterID != cluster || v.CaptureLineage != lineage) {
		return nil, errors.New("DB2 pureScale restart vector belongs to another cluster/capture lineage")
	}
	v.ClusterID = cluster
	v.CaptureLineage = lineage
	return &PureScaleReader{agent: agent, selections: sel, vector: v, resource: resource}, nil
}
func validatePureScaleStream(st PureScaleStream, cluster, lineage string) error {
	if st.ClusterID == "" || st.StreamID == "" || st.MemberID == "" || st.Epoch == 0 || st.ResolvedGlobalSequence == 0 || !validLineage(st.CaptureLineage) {
		return fmt.Errorf("DB2 pureScale stream %q is missing proof fields", st.StreamID)
	}
	if cluster != "" && (st.ClusterID != cluster || st.CaptureLineage != lineage) {
		return errors.New("DB2 pureScale streams disagree on cluster/capture lineage")
	}
	return nil
}
func (r *PureScaleReader) validateEvents(f PureScaleFragment) error {
	if !f.CompleteRowImages {
		return fmt.Errorf("DB2 pureScale fragment %s does not attest complete row images", f.TID)
	}
	if len(f.Events) == 0 {
		return errors.New("DB2 pureScale committed fragment has no events")
	}
	for _, e := range f.Events {
		if e.Operation == domain.CDCDDL || e.Operation == domain.CDCCheckpoint {
			continue
		}
		k := strings.ToLower(e.SourceSchema + "." + e.SourceTable)
		if _, ok := r.selections[k]; !ok {
			return fmt.Errorf("DB2 pureScale provider emitted unselected table %s", k)
		}
	}
	return nil
}
func (r *PureScaleReader) load(ctx context.Context) error {
	sr, err := r.agent.PureScaleStreams(ctx)
	if err != nil {
		return err
	}
	streams := make([]orderedmerge.Stream[PureScaleFragment], 0, len(sr.Streams))
	desc := map[string]PureScaleStream{}
	for _, st := range sr.Streams {
		if err := validatePureScaleStream(st, r.vector.ClusterID, r.vector.CaptureLineage); err != nil {
			return err
		}
		old, ok := r.vector.Streams[st.StreamID]
		if !ok {
			return fmt.Errorf("new DB2 pureScale stream %s appeared without planned vector", st.StreamID)
		}
		if st.Epoch != old.Epoch && st.PredecessorEpoch != old.Epoch {
			return fmt.Errorf("DB2 pureScale stream %s failover epoch has no predecessor proof", st.StreamID)
		}
		start, err := ParseLRI(old.LRI)
		if err != nil {
			return err
		}
		resp, err := r.agent.PureScaleRead(ctx, st.StreamID, start, 4096, 32<<20)
		if err != nil {
			return err
		}
		if resp.Stream.ResolvedGlobalSequence != st.ResolvedGlobalSequence || resp.Stream.Epoch != st.Epoch {
			return fmt.Errorf("DB2 pureScale stream %s descriptor changed during read", st.StreamID)
		}
		fs := []orderedmerge.Fragment[PureScaleFragment]{}
		for _, f := range resp.Fragments {
			if f.StreamID != st.StreamID || f.Epoch != st.Epoch || f.GlobalSequence == 0 {
				return fmt.Errorf("DB2 pureScale fragment proof mismatch on %s", st.StreamID)
			}
			if err := r.validateEvents(f); err != nil {
				return err
			}
			fs = append(fs, orderedmerge.Fragment[PureScaleFragment]{Stream: st.StreamID, Order: f.GlobalSequence, Value: f})
		}
		streams = append(streams, orderedmerge.Stream[PureScaleFragment]{ID: st.StreamID, Resolved: st.ResolvedGlobalSequence, Fragments: fs})
		desc[st.StreamID] = st
	}
	groups, err := orderedmerge.Merge(streams)
	if err != nil {
		return err
	}
	for _, g := range groups {
		next := PureScaleVector{ClusterID: r.vector.ClusterID, CaptureLineage: r.vector.CaptureLineage, GlobalSequence: g.Order, Streams: map[string]pureScaleVectorStream{}}
		for k, v := range r.vector.Streams {
			next.Streams[k] = v
		}
		events := []domain.CDCEvent{}
		ts := int64(0)
		for _, of := range g.Fragments {
			f := of.Value
			next.Streams[f.StreamID] = pureScaleVectorStream{Epoch: f.Epoch, LRI: f.NextLRI.String()}
			events = append(events, f.Events...)
			if f.SourceTimestampMS > ts {
				ts = f.SourceTimestampMS
			}
		}
		pos := next.String()
		for i := range events {
			events[i].PositionType = "DB2_PURESCALE_VECTOR"
			events[i].PositionValue = pos
			events[i].Resource = r.resource
		}
		r.ready = append(r.ready, &cdcruntime.Transaction{Events: events, Checkpoint: domain.CDCPosition{DatabaseType: string(domain.DataSourceDB2), PositionType: "DB2_PURESCALE_VECTOR", PositionValue: pos, Resource: r.resource, SourceTimestampMS: ts}, Label: fmt.Sprintf("db2 pureScale gcs %d", g.Order)})
	}
	return nil
}
func (r *PureScaleReader) Next(ctx context.Context) (*cdcruntime.Transaction, error) {
	for {
		r.mu.Lock()
		if len(r.ready) == 0 {
			if err := r.load(ctx); err != nil {
				r.mu.Unlock()
				return nil, err
			}
		}
		if len(r.ready) > 0 {
			tx := r.ready[0]
			r.ready = r.ready[1:]
			r.mu.Unlock()
			return tx, nil
		}
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
func (r *PureScaleReader) Acknowledge(_ context.Context, tx *cdcruntime.Transaction) error {
	if tx == nil {
		return errors.New("nil DB2 pureScale transaction")
	}
	v, err := ParsePureScaleVector(tx.Checkpoint.PositionValue)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if v.GlobalSequence < r.vector.GlobalSequence {
		return errors.New("DB2 pureScale global sequence acknowledgement regressed")
	}
	r.vector = v
	return nil
}
func (r *PureScaleReader) Close() error { return nil }
func (v PureScaleVector) StreamIDs() []string {
	out := make([]string, 0, len(v.Streams))
	for k := range v.Streams {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
