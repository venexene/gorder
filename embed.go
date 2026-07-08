package gorder

import "embed"

//go:embed web/templates/*
var TemplatesFS embed.FS

//go:embed web/static
var StaticFS embed.FS
