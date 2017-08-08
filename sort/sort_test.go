package sort

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	key, value int
}

type structs []*testStruct

func (is structs) Len() int {
	return len(is)
}

func (is structs) Less(i, j int) bool {
	return is[i].key < is[j].key
}

func (is structs) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

func (is structs) Reverse() {
	for i := 0; i < is.Len() / 2; i++ {
		j := is.Len() - 1 - i
		is.Swap(i, j)
	}
}

func newStruct(key, value int) *testStruct {
	return &testStruct{
		key: key,
		value: value,
	}
}

func TestMultithreadedSortEven(t *testing.T) {
	s1, s2, s3, s4 := newStruct(1, 1), newStruct(2, 2), newStruct(3, 4), newStruct(3, 3)
	test := structs{s2, s3, s4, s1}
	multithreadedSymSort(test)
	assert.Equal(t, structs{s1, s2, s3, s4}, test)
}

func TestMultithreadedSortOdd(t *testing.T) {
	s1, s2, s3, s4, s5 := newStruct(1, 1), newStruct(2, 2), newStruct(3, 4), newStruct(3, 3), newStruct(4, 5)
	test := structs{s2, s5, s3, s4, s1}
	multithreadedSymSort(test)
	assert.Equal(t, structs{s1, s2, s3, s4, s5}, test)
}

func BenchmarkStdLib(b *testing.B) {
	numItems := 10000000
	test := make(structs, 0, numItems)
	for i := numItems; i > 0; i-- {
		test = append(test, newStruct(i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Stable(test)
		test.Reverse()
	}
}

func BenchmarkMultithreaded(b *testing.B) {
	numItems := 10000000
	test := make(structs, 0, numItems)
	for i := numItems; i > 0; i-- {
		test = append(test, newStruct(i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		multithreadedSymSort(test)
		test.Reverse()
	}
}
