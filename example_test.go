package vexo_test

import (
	"context"
	"log"
	"os"

	"github.com/vexotech/vexo-go"
)

func Example() {
	client, err := vexo.New(os.Getenv("VEXO_API_KEY"), vexo.Config{})
	if err != nil {
		log.Fatal(err)
	}
	// Send everything still queued before the program exits.
	defer client.Close(context.Background())

	// Attach traits to the person. Use the ID you pass to identifyDevice() in the app SDK.
	if err := client.Identify(vexo.Identify{
		DistinctID: "user@example.com",
		Traits:     map[string]any{"plan": "pro", "company": "Acme"},
	}); err != nil {
		log.Print(err)
	}

	// Record an event.
	if err := client.Capture(vexo.Capture{
		DistinctID: "user@example.com",
		Event:      "purchase_completed",
		Properties: map[string]any{"amount": 49.9, "currency": "USD"},
	}); err != nil {
		log.Print(err)
	}
}
