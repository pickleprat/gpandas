package gpandas

import (
	"gpandas/dataframe"
	"gpandas/utils/collection"
)

type float_column struct {
	vals []float64
}

type string_column struct {
	vals []string
}

type int_column struct {
	vals []int64
}

func (gpandas) DataFrame(columns []string, data map[string][]interface{}) (*dataframe.DataFrame, error) {
	cols, err := dataframe.GetMapKeys(data)
	if err != nil {
		return nil, err
	}
	cols_compare := collection.NewSet[string]()
	df := &dataframe.DataFrame{data: data}
	return df, nil
}

type gpandas struct {
}
