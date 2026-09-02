package main

import (
	"testing"
)

func TestParseWorkerLabels(t *testing.T) {
	got := parseWorkerLabels(`zone=az-a,rack=r2`)
	if got["zone"] != "az-a" || got["rack"] != "r2" {
		t.Fatalf("labels=%v", got)
	}
	got = parseWorkerLabels(`{"region":"cn-hangzhou","network":"migration"}`)
	if got["region"] != "cn-hangzhou" || got["network"] != "migration" {
		t.Fatalf("json labels=%v", got)
	}
}
