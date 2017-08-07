package slice

import (
	"runtime"
	"sort"
	"sync"
)

// MultithreadedSortThreshold determines how long a slice needs to be before
// the multithreaded algorithm is used.  There is some overhead to the
// goroutines, so for really small slices the std lib sort is used.
const MultithreadedSortThreshold = 100

// ChunkSize defines the size of the subslices that are sent to the insertion
// sort algorithm.  20-30 is a good number with best empirical results.
const ChunkSize = 20

// pool is used to hold sort chunks to reduce allocations as much as possible.
// Moving to a different pattern of using defined memory locations with an array
// is a good target for future work.
var pool = sync.Pool{
	New: func() interface{} {
		return &sortChunk{}
	},
}

// Stable performs a multithreaded stable sort, if the slice is long enough
// to warrant such an approach.  Otherwise, performs a standard single-threaded
// sort using the std lib.  Also uses the standard library sort if there is only
// one cpu in the machine, not much use there.  Only runtime.NumCPU is checked
// here as more recent versions of go automatically set GOMAXPROCS to this
// value.
func Stable(data sort.Interface) {
	if data.Len() < MultithreadedSortThreshold || runtime.NumCPU() == 1 {
		sort.Stable(data)
		return
	}

	multithreadedSymSort(data)
}

// runner is the asynchronous function responsible for performing merging and
// sorting.  This function returns when closer is closed.  Any sortChunk structs
// received by the runner with a mid of 0 will be assumed to be a sort command
// and will take the insertion sort code path.  A non-zero mid will cause a
// merge.
func runner(wg *sync.WaitGroup, data sort.Interface, closer chan struct{}, sorter chan *sortChunk) {
	for {
		select {
		case <- closer:
			return
		case toSort :=<- sorter:
			if toSort.mid == 0 {
				insertionSort(
					data, toSort.start, toSort.stop,
				)
				wg.Done()
				pool.Put(toSort)
				continue
			}

			symMerge(
				data,
				toSort.start,
				toSort.mid,
				toSort.stop,
			)
			wg.Done()
			pool.Put(toSort)
		}
	}
}

// newChunk returns a sortChunk with the provided values.  The sortChunk may or
// may not be reused.
func newChunk(start, mid, stop int) *sortChunk {
	chunk := pool.Get().(*sortChunk)
	chunk.start = start
	chunk.mid = mid
	chunk.stop = stop
	return chunk
}

// sortChunk is used to send data to the runners regarding which subslices to
// act upon.
type sortChunk struct {
	start, mid, stop int
}

// multithreadedSymSort copies the sort logic from the standard library but
// parallelizes it.  This is used if the len of the slice is sufficient.  The
// symmetrical merge algorithm is a good target for concurrency.  The slice
// is broken down into sub slices which are sorted concurrently using the
// standard library's insertion sort.  The resulting subslices are then merged
// resulting in a stable sort done concurrently.  This can also be done without
// any modification to the sort.Interface interface.
func multithreadedSymSort(data sort.Interface) {
	closer := make(chan struct{}) // used to close goroutines
	defer close(closer)

	blockSize := ChunkSize

	// 1) Calculate how many chunks we're going to need and prepare
	// go routines.
	numChunks := data.Len() / blockSize + 1 // how many sub slices need to
	// be sorted the buffer size here needs to be high enough to not be too
	// blocking to the insertion loop
	chunks := make(chan *sortChunk, runtime.NumCPU() * 400)
	var wg sync.WaitGroup
	// start the runners, num cpu is usually a pretty good bet
	for i := 0; i < runtime.NumCPU(); i++ {
		go runner(&wg, data, closer, chunks)
	}

	wg.Add(numChunks)
	a, b, n := 0, blockSize, data.Len()
	for b <= n {
		chunks <- newChunk(a, 0, b)
		a = b
		b += blockSize
	}

	chunks <- newChunk(a, 0, n)

	wg.Wait() // indicates all the subslices are sorted, ready to be merged

	// all subslices are then merged iteratively, starting with the smallest
	// block size merge and ending when nothing is left to be merged
	for blockSize < n {
		a, b = 0, 2*blockSize
		wg.Add(n / (2 * blockSize))
		for b <= n {
			chunks <- newChunk(a, a+blockSize, b)
			a = b
			b += 2 * blockSize
		}
		if m := a + blockSize; m < n {
			wg.Add(1)
			chunks <- newChunk(a, m, n)
		}
		blockSize *= 2
		wg.Wait()
	}
}

// All functions below are copied/pasted from the standard library as of
// 1.8.3.

func swapRange(data sort.Interface, a, b, n int) {
	for i := 0; i < n; i++ {
		data.Swap(a+i, b+i)
	}
}

// SymMerge merges the two sorted subsequences data[a:m] and data[m:b] using
// the SymMerge algorithm from Pok-Son Kim and Arne Kutzner, "Stable Minimum
// Storage Merging by Symmetric Comparisons", in Susanne Albers and Tomasz
// Radzik, editors, Algorithms - ESA 2004, volume 3221 of Lecture Notes in
// Computer Science, pages 714-723. Springer, 2004.
//
// Let M = m-a and N = b-n. Wolog M < N.
// The recursion depth is bound by ceil(log(N+M)).
// The algorithm needs O(M*log(N/M + 1)) calls to data.Less.
// The algorithm needs O((M+N)*log(M)) calls to data.Swap.
//
// The paper gives O((M+N)*log(M)) as the number of assignments assuming a
// rotation algorithm which uses O(M+N+gcd(M+N)) assignments. The argumentation
// in the paper carries through for Swap operations, especially as the block
// swapping rotate uses only O(M+N) Swaps.
//
// symMerge assumes non-degenerate arguments: a < m && m < b.
// Having the caller check this condition eliminates many leaf recursion calls,
// which improves performance.
func symMerge(data sort.Interface, a, m, b int) {
	// Avoid unnecessary recursions of symMerge
	// by direct insertion of data[a] into data[m:b]
	// if data[a:m] only contains one element.
	if m-a == 1 {
		// Use binary search to find the lowest index i
		// such that data[i] >= data[a] for m <= i < b.
		// Exit the search loop with i == b in case no such index exists.
		i := m
		j := b
		for i < j {
			h := i + (j-i)/2
			if data.Less(h, a) {
				i = h + 1
			} else {
				j = h
			}
		}
		// Swap values until data[a] reaches the position before i.
		for k := a; k < i-1; k++ {
			data.Swap(k, k+1)
		}
		return
	}

	// Avoid unnecessary recursions of symMerge
	// by direct insertion of data[m] into data[a:m]
	// if data[m:b] only contains one element.
	if b-m == 1 {
		// Use binary search to find the lowest index i
		// such that data[i] > data[m] for a <= i < m.
		// Exit the search loop with i == m in case no such index exists.
		i := a
		j := m
		for i < j {
			h := i + (j-i)/2
			if !data.Less(m, h) {
				i = h + 1
			} else {
				j = h
			}
		}
		// Swap values until data[m] reaches the position i.
		for k := m; k > i; k-- {
			data.Swap(k, k-1)
		}
		return
	}

	mid := a + (b-a)/2
	n := mid + m
	var start, r int
	if m > mid {
		start = n - b
		r = mid
	} else {
		start = a
		r = m
	}
	p := n - 1

	for start < r {
		c := start + (r-start)/2
		if !data.Less(p-c, c) {
			start = c + 1
		} else {
			r = c
		}
	}

	end := n - start
	if start < m && m < end {
		rotate(data, start, m, end)
	}
	if a < start && start < mid {
		symMerge(data, a, start, mid)
	}
	if mid < end && end < b {
		symMerge(data, mid, end, b)
	}
}

// Rotate two consecutives blocks u = data[a:m] and v = data[m:b] in data:
// Data of the form 'x u v y' is changed to 'x v u y'.
// Rotate performs at most b-a many calls to data.Swap.
// Rotate assumes non-degenerate arguments: a < m && m < b.
func rotate(data sort.Interface, a, m, b int) {
	i := m - a
	j := b - m

	for i != j {
		if i > j {
			swapRange(data, m-i, m, j)
			i -= j
		} else {
			swapRange(data, m-i, m+j-i, i)
			j -= i
		}
	}
	// i == j
	swapRange(data, m-i, m, i)
}

func insertionSort(data sort.Interface, a, b int) {
	for i := a + 1; i < b; i++ {
		for j := i; j > a && data.Less(j, j-1); j-- {
			data.Swap(j, j-1)
		}
	}
}
