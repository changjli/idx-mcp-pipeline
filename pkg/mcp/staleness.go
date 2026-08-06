package mcp

// StalenessMetadata is embedded in every MCP tool response.
type StalenessMetadata struct {
	DataStale    bool   `json:"data_stale,omitempty"`
	LastGoodDate string `json:"last_good_date,omitempty"`
}
