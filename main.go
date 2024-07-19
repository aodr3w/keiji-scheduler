package main

import (
	"log"

	"github.com/aodr3w/keiji-core/utils"
	"github.com/aodr3w/keiji-scheduler/core"
)

func main() {
	executor, err := core.NewExecutor()
	if err != nil {
		log.Fatalf("failed to start executor due to error %v", err)
	}
	go executor.Start()

	utils.HandleStopSignal(executor.Stop)
}
