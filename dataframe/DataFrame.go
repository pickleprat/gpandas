package dataframe

import (
	"errors"
	"sync"
)

func get_map_keys(input_map map[interface{}]interface{}) ([]interface{}, error){
	i := 0
	keys := make([]interface{}, 0, len(input_map))
	for k := range input_map {
		keys[i] = k
		i++
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
	keys := make([]string, 0, len(columns))
	i := 0
	for k := range columns {
		keys[i] = k
		i++
	}
	for colu

	df.Lock()
	df.Unlock()
	return nil

}
