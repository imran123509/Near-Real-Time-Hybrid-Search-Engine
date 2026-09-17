package kafka

import "testing"

func TestOffsetTrackerCommitsOnlyContiguousOffsets(t *testing.T) {
	tr := newOffsetTracker()
	for off := int64(10); off <= 13; off++ {
		tr.add("events", 0, off)
	}

	steps := []struct {
		done       int64
		wantOffset int64
		wantOK     bool
	}{
		{done: 12, wantOK: false},                // 10 and 11 still in flight
		{done: 10, wantOffset: 10, wantOK: true}, // 10 is now safe
		{done: 13, wantOK: false},                // 11 still in flight
		{done: 11, wantOffset: 13, wantOK: true}, // 11, 12, 13 all finished
		{done: 99, wantOffset: 0, wantOK: false}, // unknown offset changes nothing
	}
	for _, s := range steps {
		got, ok := tr.markDone("events", 0, s.done)
		if ok != s.wantOK || (ok && got != s.wantOffset) {
			t.Fatalf("markDone(%d) = (%d, %v), want (%d, %v)", s.done, got, ok, s.wantOffset, s.wantOK)
		}
	}
}

func TestOffsetTrackerKeepsPartitionsIndependent(t *testing.T) {
	tr := newOffsetTracker()
	tr.add("events", 0, 5)
	tr.add("events", 1, 5)
	tr.add("events", 0, 6)

	if _, ok := tr.markDone("events", 0, 6); ok {
		t.Fatal("partition 0 committed past in-flight offset 5")
	}
	if got, ok := tr.markDone("events", 1, 5); !ok || got != 5 {
		t.Fatalf("partition 1 = (%d, %v), want (5, true)", got, ok)
	}
	if got, ok := tr.markDone("events", 0, 5); !ok || got != 6 {
		t.Fatalf("partition 0 = (%d, %v), want (6, true)", got, ok)
	}
	if _, ok := tr.markDone("other", 0, 5); ok {
		t.Fatal("untracked topic reported a commit")
	}
}
