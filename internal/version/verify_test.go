package version

import "testing"

func TestVersionBatchNoDuplicate(t *testing.T) {
	alloc := NewAllocator(1)
	first := alloc.AllocBatch(3)
	second := alloc.AllocBatch(3)
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("batches must contain exactly 3 versions each: first=%v second=%v", first, second)
	}
	seen := make(map[uint64]bool)
	for _, value := range append(append([]uint64{}, first...), second...) {
		if seen[value] {
			t.Fatalf("version %d was handed out twice across batches %v and %v", value, first, second)
		}
		seen[value] = true
	}
	for index := 1; index < len(first); index++ {
		if first[index] != first[index-1]+1 {
			t.Fatalf("first batch is not contiguous: %v", first)
		}
	}
	for index := 1; index < len(second); index++ {
		if second[index] != second[index-1]+1 {
			t.Fatalf("second batch is not contiguous: %v", second)
		}
	}
}
