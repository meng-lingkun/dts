package mysqlconnector

import "testing"

func TestRC39TiDBStoreLabelsExposeFaultDomains(t *testing.T) {
	labels := map[string]string{}
	mergeTiDBStoreTopologyLabels(labels, `[{"key":"zone","value":"az-a"},{"key":"rack","value":"rack-7"},{"key":"region","value":"sg"}]`)
	if labels["region"] != "sg" || labels["zone"] != "az-a" || labels["rack"] != "rack-7" {
		t.Fatalf("parsed labels=%v", labels)
	}
	labels = map[string]string{}
	mergeTiDBStoreTopologyLabels(labels, `zone=az-b,rack=rack-9`)
	if labels["zone"] != "az-b" || labels["rack"] != "rack-9" {
		t.Fatalf("parsed compact labels=%v", labels)
	}
}
