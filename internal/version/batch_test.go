package version

import "testing"

// TestAllocBatchNoOverlap guards against the off-by-one where two batches
// shared their boundary version: AllocBatch must reserve exactly n versions
// and leave the allocator pointing past the last one it handed out, so the
// next allocation (batch or single) can never reissue a version.
func TestAllocBatchNoOverlap(t *testing.T) {
	alloc := NewAllocator(1)

	first := alloc.AllocBatch(2)
	if len(first) != 2 || first[0] != 1 || first[1] != 2 {
		t.Fatalf("first AllocBatch(2) = %v, want [1 2]", first)
	}

	// A single Next immediately after the batch must not collide with any
	// version the batch just issued.
	single := alloc.Next()
	if single != 3 {
		t.Fatalf("Next after batch = %d, want 3 (batch must not leak its last version)", single)
	}

	second := alloc.AllocBatch(2)
	if len(second) != 2 || second[0] != 4 || second[1] != 5 {
		t.Fatalf("second AllocBatch(2) = %v, want [4 5] (no overlap with first batch)", second)
	}

	// Collect every issued version and assert uniqueness across batch + single.
	issued := append([]uint64{}, first...)
	issued = append(issued, single)
	issued = append(issued, second...)
	seen := make(map[uint64]struct{}, len(issued))
	for _, v := range issued {
		if _, dup := seen[v]; dup {
			t.Fatalf("duplicate version %d issued across allocations", v)
		}
		seen[v] = struct{}{}
	}
}

// TestAllocBatchSize ensures the slice length matches the request exactly for
// the edge cases that previously fed the off-by-one (n == 1 and n == 0).
func TestAllocBatchSize(t *testing.T) {
	alloc := NewAllocator(10)

	one := alloc.AllocBatch(1)
	if len(one) != 1 || one[0] != 10 {
		t.Fatalf("AllocBatch(1) = %v, want [10]", one)
	}

	zero := alloc.AllocBatch(0)
	if zero != nil {
		t.Fatalf("AllocBatch(0) = %v, want nil", zero)
	}

	// After AllocBatch(1) the next version must be strictly greater than the
	// one handed out.
	if next := alloc.Next(); next != 11 {
		t.Fatalf("Next after AllocBatch(1) = %d, want 11", next)
	}
}

// TestRangeHalfOpen documents the [start, end) contract that AllocBatch relies
// on: end is exclusive, so Range(1, 4) yields exactly three values.
func TestRangeHalfOpen(t *testing.T) {
	r := Range(1, 4)
	if len(r) != 3 || r[0] != 1 || r[1] != 2 || r[2] != 3 {
		t.Fatalf("Range(1,4) = %v, want [1 2 3]", r)
	}
	if Range(5, 5) != nil {
		t.Fatalf("Range(5,5) should be empty, got %v", Range(5, 5))
	}
	if Range(6, 4) != nil {
		t.Fatalf("Range(6,4) should be empty, got %v", Range(6, 4))
	}
}
