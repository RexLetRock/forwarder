package main

import (
	"flag"
	"fmt"
	"forwarder/forwarder"
)

func main() {
	port := *flag.String("port", "10000", "port number")
	flag.Parse()
	forwarder.ServerStart(fmt.Sprintf("0.0.0.0:%v", port))
}
