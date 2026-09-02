package orderedmerge

import "testing"

func TestMergeRequiresAllWatermarksAndGroupsSameOrder(t *testing.T) {
	_, err := Merge([]Stream[string]{{ID: "a", Resolved: 10}, {ID: "b"}})
	if err == nil {
		t.Fatal("expected missing watermark rejection")
	}
	got, err := Merge([]Stream[string]{
		{ID: "a", Resolved: 12, Fragments: []Fragment[string]{{Order: 10, Value: "a10"}, {Order: 13, Value: "a13"}}},
		{ID: "b", Resolved: 11, Fragments: []Fragment[string]{{Order: 10, Value: "b10"}, {Order: 11, Value: "b11"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Order != 10 || len(got[0].Fragments) != 2 || got[1].Order != 11 {
		t.Fatalf("unexpected groups %#v", got)
	}
}
