package main

import (
	"fmt"
	"gpandas"
	"time"
)

func maitrn() {
	start := time.Now()
	gp := gpandas.GoPandas{}
	df, err := gp.From_gbq("SELECT distinct(Card_Name) FROM `jm-ebg.NewsFeed.NewsFeedCardsTracker`", "jm-ebg")
	if err != nil {
		fmt.Printf("Error while querying => %v", err)
	}
	fmt.Printf("##########")
	fmt.Printf("%v", df.String())
	fmt.Printf("##########")
	elapsed := time.Since(start)
	fmt.Printf("%f\n", elapsed.Seconds())
}
