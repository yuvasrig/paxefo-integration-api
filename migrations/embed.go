// Package migrations embeds the goose SQL migrations so the binary can migrate at startup (F13-R1).
package migrations

import "embed"

// FS holds the *.sql migrations.
//
//go:embed *.sql
var FS embed.FS
