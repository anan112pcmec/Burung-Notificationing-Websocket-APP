package main

import (
	"flag"
	"fmt"

	notification_app "burung-notificationing-app/notification-app"
)

func main() {

	var rebootcass bool
	flag.BoolVar(&rebootcass, "cassreboot", false, "contoh penggunaan")
	flag.Parse()

	if rebootcass {
		fmt.Println("akan restart cassandra")
	} else {
		fmt.Println("cassandra gaakan di restart")
	}
	notification_app.RunApp(rebootcass)
}
