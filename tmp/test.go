package main

import "fmt"

func main() {
	//var numbers = make([]int,3,5)
	interal := []int{}
	interal = append(interal, 3)
	printSlice(interal)
}

func printSlice(x []int) {
	fmt.Printf("len=%d cap=%d slice=%v\n", len(x), cap(x), x)
}
