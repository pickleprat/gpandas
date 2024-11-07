package collection

import "errors"

// Set is an alias for a map representing an unordered collection of unique elements of type T.
// Type T must be comparable.
type Set[T comparable] map[T]struct{}

// NewSet creates and initializes a new empty Set.
// If an initial size is provided, the set will be pre-allocated with that size.
// The initial size must be a non-negative integer.
func NewSet[T comparable](initialSize ...int) (Set[T], error) {
	if len(initialSize) > 0 {
		if initialSize[0] < 0 {
			return nil, errors.New("initialSize must be a non-negative integer")
		}
		return make(Set[T], initialSize[0]), nil
	}
	return make(Set[T]), nil
}

// Has returns true if the value exists in the set.
func (s Set[T]) Has(v T) bool {
	_, ok := s[v]
	return ok
}

// Add inserts a value into the set only if it does not already exist.
func (s Set[T]) Add(v T) error {
	if !s.Has(v) {
		s[v] = struct{}{}
		return nil
	}
	return errors.New("value already exists in the set")
}

// AddMulti adds multiple values to the set at once.
func (s Set[T]) AddMulti(val ...T) error {
	for _, v := range val {
		err := s.Add(v)
		if err != nil {
			return err
		}
	}
	return nil
}

// FilterFunc defines a function type that returns true if a value should be included in filtered results.
type FilterFunc[T any] func(v T) bool

// Filter creates a new Set containing only the elements that satisfy the predicate P.
func (s Set[T]) Filter(P FilterFunc[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		if !P(v) {
			continue
		}
		res[v] = struct{}{}
	}
	return res, nil
}

// Union creates a new Set containing all elements from both sets.
func (s Set[T]) Union(s2 Set[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		res[v] = struct{}{}
	}
	for v := range s2 {
		res[v] = struct{}{}
	}
	return res, nil
}

// Slice converts a Set of type T into a slice of type []T.
// It iterates over the elements of the set and appends them to a slice.
// The resulting slice contains all the elements from the set in an arbitrary order.
//
// Parameters:
//   s: A Set[T] from which to create the slice.
//
// Returns:
//   A slice of type []T containing all elements from the input set.
func ToSlice[T comparable](s Set[T]) ([]T, error) {
	result_slice := make([]T, 0, len(s))
	for k := range s {
		result_slice = append(result_slice, k)
	}
	return result_slice, nil
}

func ToSet[T comparable](slice []T) (Set[T], error) {
	result_set, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for i := range slice {
		result_set[slice[i]] = struct{}{}
	}
	return result_set, nil
}

// Intersect creates a new Set containing only elements that exist in both sets.
func (s Set[T]) Intersect(s2 Set[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		if _, ok := s2[v]; !ok {
			continue
		}
		res[v] = struct{}{}
	}
	return res, nil
}

// Difference creates a new Set containing elements that exist in the first set but not in the second.
func (s Set[T]) Difference(s2 Set[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		if _, ok := s2[v]; ok {
			continue
		}
		res[v] = struct{}{}
	}
	return res, nil
}

func (s Set[T]) Compare(s2 Set[T]) (bool, any) {
	// Check if lengths are the same
	if len(s) != len(s2) {
		return false, nil
	}

	// Check if all keys in s are in s2
	for item := range s {
		if _, exists := s2[item]; !exists {
			return false, item
		}
	}

	return true, nil
}
