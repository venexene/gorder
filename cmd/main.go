// Binary gorder is the entry point for the order processing service.
package main

import (
	"log/slog"
	"os"

	"github.com/venexene/gorder/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		slog.Error("failed to run app", "error", err)
		os.Exit(1)
	}
}
