package repository

import "context"

type MetadataRelationStats struct {
	Relation   string
	TotalBytes int64
	TableBytes int64
	IndexBytes int64
	LiveRows   int64
	DeadRows   int64
}

func (s MetadataRelationStats) DeadRatio() float64 {
	total := s.LiveRows + s.DeadRows
	if total <= 0 {
		return 0
	}
	return float64(s.DeadRows) / float64(total)
}

type MetadataStorageStats struct {
	TotalBytes int64
	Relations  []MetadataRelationStats
}

type MetadataStatsProvider interface {
	MetadataStorageStats(context.Context) (MetadataStorageStats, error)
}

func ReadMetadataStorageStats(ctx context.Context, repo Repository) (MetadataStorageStats, error) {
	if p, ok := repo.(MetadataStatsProvider); ok {
		return p.MetadataStorageStats(ctx)
	}
	return MetadataStorageStats{}, nil
}
