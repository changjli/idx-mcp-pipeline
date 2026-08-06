package embed

import (
	"encoding/json"
	"embed"
)

//go:embed tickers.json
var tickersFS embed.FS

type TickerEntry struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

func LoadTickers() ([]TickerEntry, error) {
	data, err := tickersFS.ReadFile("tickers.json")
	if err != nil {
		return nil, err
	}
	var tickers []TickerEntry
	if err := json.Unmarshal(data, &tickers); err != nil {
		return nil, err
	}
	return tickers, nil
}
