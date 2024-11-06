package dataframe

import (
	"errors"
	"gpandas/utils/collection"
	"sync"
)

func GetMapKeys[K comparable, V any](input_map map[K]V) (collection.Set[K], error) {
	keys, err := collection.NewSet[K]()
	if err != nil {
		return nil, err
	}
	for k := range input_map {
		keys.Add(k)
	}
	return keys, nil
}

type DataFrame struct {
	sync.Mutex
	len          int64
	column_names []string
	data         map[string][]interface{}
}

func rename(df *DataFrame, columns map[string]string) error {
	if len(columns) == 0 {
		return errors.New("'columns' slice is empty. Slice of Maps to declare columns to rename is required")
	}
	keys, err := GetMapKeys(columns)
	if err != nil {
		return err
	}

	df.Lock()
	df.Unlock()
	return nil

}
