package embed

import (
	"encoding/json"
	"embed"
)

//go:embed brokers.json
var brokersFS embed.FS

type BrokerEntry struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

func LoadBrokers() ([]BrokerEntry, error) {
	data, err := brokersFS.ReadFile("brokers.json")
	if err != nil {
		return nil, err
	}
	var brokers []BrokerEntry
	if err := json.Unmarshal(data, &brokers); err != nil {
		return nil, err
	}
	return brokers, nil
}
