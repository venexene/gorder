// Binary gorder is the entry point for the order processing service.
//
// @title           Gorder API
// @version         1.0
// @description     Event-driven order processing service with JWT authentication.
// @host            localhost:8080
// @BasePath        /
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
package main

import (
	"log/slog"
	"os"

	"github.com/venexene/gorder/internal/app"
	_ "github.com/venexene/gorder/docs"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	dep := &app.Dependencies{}
	dep.Version = version
	dep.Commit = commit

	if err := app.Run(dep); err != nil {
		slog.Error("failed to run app", "error", err)
		os.Exit(1)
	}
}
