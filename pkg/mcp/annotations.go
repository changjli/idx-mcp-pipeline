package mcp

// ToolAnnotations declares MCP tool metadata for client-side behavior hints.
type ToolAnnotations struct {
	ReadOnlyHint    bool `json:"readOnlyHint"`
	DestructiveHint bool `json:"destructiveHint"`
	OpenWorldHint   bool `json:"openWorldHint"`
}

// DefaultAnnotations returns the standard read-only tool annotations.
func DefaultAnnotations() ToolAnnotations {
	return ToolAnnotations{
		ReadOnlyHint:    true,
		DestructiveHint: false,
		OpenWorldHint:   true,
	}
}
