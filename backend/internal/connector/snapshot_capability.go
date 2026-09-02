package connector

import (
	"context"
	"qmigration/backend/internal/domain"
	"strings"
)

// SnapshotCapability returns a truthful capability level even for connectors
// predating ValidationSnapshotCapabilityProvider.
func SnapshotCapability(ctx context.Context, c Connector, pos domain.CDCPosition) ValidationSnapshotCapability {
	if p, ok := c.(ValidationSnapshotCapabilityProvider); ok {
		return p.ValidationSnapshotCapability(ctx, pos)
	}
	if _, ok := c.(ValidationSnapshotConnector); ok {
		return ValidationSnapshotCapability{Level: ValidationSnapshotExactHistorical, PositionTypes: []string{strings.ToUpper(strings.TrimSpace(pos.PositionType))}}
	}
	return ValidationSnapshotCapability{Level: ValidationSnapshotUnavailable, Note: "connector has no exact historical snapshot provider"}
}
