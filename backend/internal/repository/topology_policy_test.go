package repository

import (
	"testing"

	"qmigration/backend/internal/domain"
)

func TestRC38EffectiveTopologyRecoveryCap(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_DEGRADED_MAX_CONCURRENCY", "1")
	t.Setenv("QMIGRATION_TOPOLOGY_RECOVERY_MAX_CONCURRENCY", "4")
	table := &domain.MigrationTable{TopologyPerformance: map[string]domain.TableTopologyPerformance{
		"dn-a": {Health: "DEGRADED", RecoveryConcurrencyCap: 3},
	}}
	if got := TopologyEffectiveConcurrencyCap(table, "dn-a"); got != 3 {
		t.Fatalf("recovery cap=%d want=3", got)
	}
	if !TopologyClaimAllowed(table, "dn-a", 2) {
		t.Fatal("third recovery claim should be allowed while running=2 cap=3")
	}
	if TopologyClaimAllowed(table, "dn-a", 3) {
		t.Fatal("claim must stop at recovery cap=3")
	}
	table.TopologyPerformance["dn-a"] = domain.TableTopologyPerformance{Health: "CIRCUIT_OPEN", RecoveryConcurrencyCap: 4}
	if got := TopologyEffectiveConcurrencyCap(table, "dn-a"); got != 0 {
		t.Fatalf("open circuit cap=%d want=0", got)
	}
}

func TestRC39CanonicalFaultDomainsAndPeerRisk(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION", "true")
	fd := CanonicalFaultDomain(map[string]string{"cloud_region": "sg", "availability_zone": "az-1", "rack_id": "rack-2"})
	if fd["region"] != "sg" || fd["zone"] != "sg/az-1" || fd["rack"] != "sg/az-1/rack-2" {
		t.Fatalf("canonical fault domain=%v", fd)
	}
	table := &domain.MigrationTable{
		Topology: []domain.TopologyPlacement{
			{ID: "dn-a", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r1"}},
			{ID: "dn-b", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r2"}},
			{ID: "dn-c", Labels: map[string]string{"region": "sg", "zone": "az-2", "rack": "r1"}},
		},
		TopologyPerformance: map[string]domain.TableTopologyPerformance{
			"dn-a": {Health: "HEALTHY"},
			"dn-b": {Health: "CIRCUIT_OPEN"},
			"dn-c": {Health: "HEALTHY"},
		},
	}
	if risk := TopologyFaultDomainPeerRisk(table, "dn-a"); risk != 3 {
		t.Fatalf("same-zone peer risk=%d want=3", risk)
	}
	if risk := TopologyFaultDomainPeerRisk(table, "dn-c"); risk != 0 {
		t.Fatalf("single bad zone must not poison the whole region, risk=%d", risk)
	}
	table.Topology = append(table.Topology, domain.TopologyPlacement{ID: "dn-d", Labels: map[string]string{"region": "sg", "zone": "az-3", "rack": "r3"}})
	table.TopologyPerformance["dn-d"] = domain.TableTopologyPerformance{Health: "DEGRADED"}
	if risk := TopologyFaultDomainPeerRisk(table, "dn-c"); risk != 3 {
		t.Fatalf("two unhealthy zones should escalate region risk; got=%d want=3", risk)
	}
	if cap := FaultDomainConcurrencyCap(3); cap != 1 {
		t.Fatalf("critical fault-domain cap=%d want=1", cap)
	}
}

func TestRC40FaultDomainPeerStateReturnsQualifiedScope(t *testing.T) {
	t.Setenv("QMIGRATION_TOPOLOGY_FAULT_DOMAIN_PROTECTION", "true")
	table := &domain.MigrationTable{
		Topology: []domain.TopologyPlacement{
			{ID: "dn-a", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r1"}},
			{ID: "dn-b", Labels: map[string]string{"region": "sg", "zone": "az-1", "rack": "r2"}},
			{ID: "dn-c", Labels: map[string]string{"region": "sg", "zone": "az-2", "rack": "r3"}},
			{ID: "dn-d", Labels: map[string]string{"region": "sg", "zone": "az-3", "rack": "r4"}},
		},
		TopologyPerformance: map[string]domain.TableTopologyPerformance{
			"dn-a": {Health: "HEALTHY"},
			"dn-b": {Health: "CIRCUIT_OPEN"},
			"dn-c": {Health: "HEALTHY"},
			"dn-d": {Health: "HEALTHY"},
		},
	}
	if risk, scope := TopologyFaultDomainPeerState(table, "dn-a"); risk != 3 || scope != "zone" {
		t.Fatalf("same-zone state risk=%d scope=%q want=3/zone", risk, scope)
	}
	if risk, scope := TopologyFaultDomainPeerState(table, "dn-c"); risk != 0 || scope != "" {
		t.Fatalf("single unhealthy zone must not produce region state: risk=%d scope=%q", risk, scope)
	}
	table.TopologyPerformance["dn-d"] = domain.TableTopologyPerformance{Health: "DEGRADED"}
	if risk, scope := TopologyFaultDomainPeerState(table, "dn-c"); risk != 3 || scope != "region" {
		t.Fatalf("multi-zone region state risk=%d scope=%q want=3/region", risk, scope)
	}
}
