package main

import (
	"fmt"
	"gpandas"
	"time"
)

func main() {
	start := time.Now()
	gp := gpandas.GoPandas{}
	df, err := gp.From_gbq("SELECT * FROM `jm-ebg.Brokerage2024.RevenueNew`", "jm-ebg")
	if err != nil {
		fmt.Printf("Error reading from gbq: %v\n", err)
		return
	}
	fmt.Println(df)
	end := time.Since(start)
	fmt.Printf("%f\n", end.Seconds())

}
