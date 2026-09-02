package repository

import (
	"os"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/perfmodel"
	"strconv"
	"strings"
)

func positiveEnvInt(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}

// TopologyHealthyMaxConcurrency returns 0 for unlimited healthy-topology concurrency.
func TopologyHealthyMaxConcurrency() int {
	return positiveEnvInt("QMIGRATION_TOPOLOGY_HEALTHY_MAX_CONCURRENCY", 0)
}

func TopologyDegradedMaxConcurrency() int {
	n := positiveEnvInt("QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY", 1)
	if n <= 0 {
		return 1
	}
	return n
}

func TopologyRecoveryMaxConcurrency() int {
	healthy := TopologyHealthyMaxConcurrency()
	fallback := 4
	if healthy > 0 {
		fallback = healthy
	}
	n := positiveEnvInt("QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY", fallback)
	base := TopologyDegradedMaxConcurrency()
	if n < base {
		n = base
	}
	if healthy > 0 && n > healthy {
		n = healthy
	}
	return n
}

func TopologyEffectiveConcurrencyCap(table *domain.MigrationTable, topologyID string) int {
	health := TopologyProfileHealth(table, topologyID)
	base := perfmodel.TopologyConcurrencyCap(health, TopologyHealthyMaxConcurrency(), TopologyDegradedMaxConcurrency())
	if health != "DEGRADED" || table == nil || table.TopologyPerformance == nil {
		return base
	}
	profile, ok := table.TopologyPerformance[topologyID]
	if !ok || profile.RecoveryConcurrencyCap <= base {
		return base
	}
	maxCap := TopologyRecoveryMaxConcurrency()
	if profile.RecoveryConcurrencyCap > maxCap {
		return maxCap
	}
	return profile.RecoveryConcurrencyCap
}

func TopologyProfileHealth(table *domain.MigrationTable, topologyID string) string {
	if table == nil || strings.TrimSpace(topologyID) == "" || table.TopologyPerformance == nil {
		return "HEALTHY"
	}
	p, ok := table.TopologyPerformance[topologyID]
	if !ok {
		return "HEALTHY"
	}
	return perfmodel.NormalizeTopologyHealth(p.Health)
}

func TopologyClaimRank(table *domain.MigrationTable, topologyID string) int {
	return perfmodel.TopologyHealthRank(TopologyProfileHealth(table, topologyID))
}

func TopologyClaimAllowed(table *domain.MigrationTable, topologyID string, running int) bool {
	if strings.TrimSpace(topologyID) == "" {
		return true
	}
	health := TopologyProfileHealth(table, topologyID)
	cap := TopologyEffectiveConcurrencyCap(table, topologyID)
	if cap == 0 {
		return health != "CIRCUIT_OPEN" // healthy cap 0 means unlimited; open cap 0 means blocked
	}
	return running < cap
}

func envDefaultOn(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	return v == "" || v == "1" || v == "true" || v == "yes" || v == "on"
}

func FaultDomainProtectionEnabled() bool {
	return envDefaultOn("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION")
}

func FaultDomainDegradedMaxConcurrency() int {
	n := positiveEnvInt("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_DEGRADED_MAX_CONCURRENCY", 2)
	if n <= 0 {
		return 1
	}
	return n
}

func FaultDomainCriticalMaxConcurrency() int {
	n := positiveEnvInt("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_CRITICAL_MAX_CONCURRENCY", 1)
	if n <= 0 {
		return 1
	}
	return n
}

func FaultDomainRegionMinUnhealthyZones() int {
	n := positiveEnvInt("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_REGION_MIN_UNHEALTHY_ZONES", 2)
	if n < 2 {
		return 2
	}
	return n
}

func firstLabel(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		for actual, value := range labels {
			if strings.EqualFold(strings.TrimSpace(actual), key) && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

// CanonicalFaultDomain normalizes connector-specific placement labels into a
// hierarchy that can be compared safely across topology implementations. Zone
// and rack values are qualified by their parents when available so two racks
// called "rack-1" in different zones are not accidentally treated as one domain.
func CanonicalFaultDomain(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	region := firstLabel(labels, "region", "cloud_region", "failure_domain_region", "topology.kubernetes.io/region")
	zoneRaw := firstLabel(labels, "zone", "az", "availability_zone", "ob_zone", "failure_domain_zone", "topology.kubernetes.io/zone")
	rackRaw := firstLabel(labels, "rack", "rack_id", "failure_domain_rack", "topology.kubernetes.io/rack")
	out := map[string]string{}
	if region != "" {
		out["region"] = region
	}
	if zoneRaw != "" {
		if region != "" {
			out["zone"] = region + "/" + zoneRaw
		} else {
			out["zone"] = zoneRaw
		}
	}
	if rackRaw != "" {
		prefix := out["zone"]
		if prefix == "" {
			prefix = region
		}
		if prefix != "" {
			out["rack"] = prefix + "/" + rackRaw
		} else {
			out["rack"] = rackRaw
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func FaultDomainsOverlap(a, b map[string]string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	for _, key := range []string{"rack", "zone", "region"} {
		av, bv := strings.TrimSpace(a[key]), strings.TrimSpace(b[key])
		if av != "" && bv != "" && av == bv {
			return true
		}
	}
	return false
}

func TopologyFaultDomain(table *domain.MigrationTable, topologyID string) map[string]string {
	if table == nil || strings.TrimSpace(topologyID) == "" {
		return nil
	}
	for _, placement := range table.Topology {
		if placement.ID == topologyID {
			return CanonicalFaultDomain(placement.Labels)
		}
	}
	return nil
}

// TopologyFaultDomainPeerState reports the correlated peer risk and the scope
// whose concurrency budget must be enforced. Rack/zone evidence is immediate;
// region evidence requires unhealthy peers in at least N distinct zones.
// Scope intentionally follows the broadest qualified domain (region > zone >
// rack), matching the Memory/PostgreSQL claim admission logic.
func TopologyFaultDomainPeerState(table *domain.MigrationTable, topologyID string) (int, string) {
	if !FaultDomainProtectionEnabled() || table == nil {
		return 0, ""
	}
	candidate := TopologyFaultDomain(table, topologyID)
	if len(candidate) == 0 {
		return 0, ""
	}
	rackRisk, zoneRisk, regionRisk := 0, 0, 0
	regionZones := map[string]bool{}
	for peerID, profile := range table.TopologyPerformance {
		if peerID == topologyID {
			continue
		}
		rank := perfmodel.TopologyHealthRank(profile.Health)
		if rank <= 0 {
			continue
		}
		peer := TopologyFaultDomain(table, peerID)
		if candidate["rack"] != "" && peer["rack"] == candidate["rack"] && rank > rackRisk {
			rackRisk = rank
		}
		if candidate["zone"] != "" && peer["zone"] == candidate["zone"] && rank > zoneRisk {
			zoneRisk = rank
		}
		if candidate["region"] != "" && peer["region"] == candidate["region"] {
			zoneKey := peer["zone"]
			if zoneKey == "" {
				zoneKey = "topology:" + peerID
			}
			regionZones[zoneKey] = true
			if rank > regionRisk {
				regionRisk = rank
			}
		}
	}
	if len(regionZones) < FaultDomainRegionMinUnhealthyZones() {
		regionRisk = 0
	}
	if regionRisk > 0 {
		return regionRisk, "region"
	}
	if zoneRisk > 0 {
		return zoneRisk, "zone"
	}
	if rackRisk > 0 {
		return rackRisk, "rack"
	}
	return 0, ""
}

// TopologyFaultDomainPeerRisk is the ranking-only compatibility helper used by
// claim ordering, throttling and Prometheus surfaces.
func TopologyFaultDomainPeerRisk(table *domain.MigrationTable, topologyID string) int {
	risk, _ := TopologyFaultDomainPeerState(table, topologyID)
	return risk
}

// FaultDomainConcurrencyCap returns 0 when no domain-level cap is required.
func FaultDomainConcurrencyCap(peerRisk int) int {
	if !FaultDomainProtectionEnabled() || peerRisk <= 0 {
		return 0
	}
	if peerRisk >= perfmodel.TopologyHealthRank("HALF_OPEN") {
		return FaultDomainCriticalMaxConcurrency()
	}
	return FaultDomainDegradedMaxConcurrency()
}
