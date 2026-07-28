// Package gorder embeds web templates and static assets into the binary.
package gorder

import "embed"

// TemplatesFS embeds HTML templates from web/templates into the binary.
//
//go:embed web/templates/*
var TemplatesFS embed.FS

// StaticFS embeds static assets (CSS, JS) from web/static into the binary.
//
//go:embed web/static
var StaticFS embed.FS
