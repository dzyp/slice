package slice

import (
	"sort"
	"sync"
	"runtime"
)

const MultithreadedSort = 100
const ChunkSize = 20
var pool = sync.Pool{
	New: func() interface{} {
		return &sortChunk{}
	},
}


func Stable(data sort.Interface) {
	if data.Len() < MultithreadedSort {
		stdLibSort(data)
		return
	}

	multithreadedThreadedSymSort(data)
}

func stdLibSort(ifc sort.Interface) {
	sort.Stable(ifc)
}

func runner(closer chan struct{}, sorter chan *sortChunk) {
	for {
		select {
		case <- closer:
			return
		case toSort :=<- sorter:
			if toSort.mid == 0 {
				insertionSort(
					toSort.data, toSort.start, toSort.stop,
				)
				toSort.wg.Done()
				pool.Put(toSort)
				continue
			}

			symMerge(
				toSort.data,
				toSort.start,
				toSort.mid,
				toSort.stop,
			)
			toSort.wg.Done()
			pool.Put(toSort)
		}
	}
}

func newChunk(wg *sync.WaitGroup, data sort.Interface, start, mid, stop int) *sortChunk {
	chunk := pool.Get().(*sortChunk)
	chunk.wg = wg
	chunk.data = data
	chunk.start = start
	chunk.mid = mid
	chunk.stop = stop
	return chunk
}

type sortChunk struct {
	wg *sync.WaitGroup
	data sort.Interface
	start, mid, stop int
}

func multithreadedThreadedSymSort(data sort.Interface) {
	closer := make(chan struct{}) // used to close goroutines
	defer close(closer)

	blockSize := ChunkSize

	// 1) Calculate how many chunks we're going to need and prepare
	// go routines.
	numChunks := data.Len() / blockSize + 1
	chunks := make(chan *sortChunk, numChunks)
	for i := 0; i < runtime.NumCPU(); i++ {
		go runner(closer, chunks)
	}

	var wg sync.WaitGroup
	wg.Add(cap(chunks))
	a, b, n := 0, blockSize, data.Len()
	for b <= n {
		chunks <- newChunk(&wg, data, a, 0, b)
		a = b
		b += blockSize
	}

	chunks <- newChunk(&wg, data, a, 0, n)

	wg.Wait()

	for blockSize < n {
		a, b = 0, 2*blockSize
		for b <= n {
			wg.Add(1)
			chunks <- newChunk(&wg, data, a, a+blockSize, b)
			a = b
			b += 2 * blockSize
		}
		if m := a + blockSize; m < n {
			wg.Add(1)
			chunks <- newChunk(&wg, data, a, m, n)
		}
		blockSize *= 2
		wg.Wait()
	}
}

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
