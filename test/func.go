package main

import (
	"fmt"
	"reflect"
)

type cc struct {
	a int
	b string
}

func main() {
	d := new(cc)
	fmt.Println(reflect.TypeOf(d))
}
