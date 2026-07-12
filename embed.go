// Package gorder embeds web templates and static assets into the binary.
package gorder

import "embed"

//go:embed web/templates/*
var TemplatesFS embed.FS

//go:embed web/static
var StaticFS embed.FS
