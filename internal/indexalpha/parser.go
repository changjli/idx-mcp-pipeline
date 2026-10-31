package indexalpha

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// wireRow is one broker row as Index Alpha sends it: volumes in shares, values
// as JSON numbers that arrive in the integral `x.0` form, averages genuinely
// fractional. Values must be float64 — unmarshalling `597425000.0` into int64
// fails.
type wireRow struct {
	Code       string  `json:"code"`
	BuyFreq    int64   `json:"buy_freq"`
	BuyVolume  int64   `json:"buy_volume"`
	BuyValue   float64 `json:"buy_value"`
	SellFreq   int64   `json:"sell_freq"`
	SellVolume int64   `json:"sell_volume"`
	SellValue  float64 `json:"sell_value"`
	BuyAvg     float64 `json:"buy_avg"`
	SellAvg    float64 `json:"sell_avg"`
}

// wireEnvelope is the shared response frame. Data and the error fields stay raw
// because the single-ticker endpoint returns an array, the batch endpoints
// return an object keyed by ticker, and `detail` is a string on some failures
// and an array on others.
type wireEnvelope struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   json.RawMessage `json:"error"`
	Detail  json.RawMessage `json:"detail"`
}

// ParseBrokerSummary parses the single-ticker broker-summary response
// (GET /stocks/broker-summary) into a source-neutral result.
//
// The conversion enforces the source invariants: shares become lots (÷100), a
// zero side contributes no row, rank is dense by value within its side, and the
// totals are derived from the full set. An empty day is an empty result, not an
// error.
func ParseBrokerSummary(ticker string, body []byte) (*broker.Result, error) {
	var env wireEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("%w: decode response: %v", ErrMalformedPayload, err)
	}
	if !env.Success {
		return nil, fmt.Errorf("%w: success=false error=%s", ErrMalformedPayload, rawText(env.Error))
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return &broker.Result{Buyers: []broker.Row{}, Sellers: []broker.Row{}}, nil
	}

	var rows []wireRow
	if err := json.Unmarshal(env.Data, &rows); err != nil {
		return nil, fmt.Errorf("%w: data is not a broker row array: %v", ErrMalformedPayload, err)
	}
	return buildResult(ticker, rows)
}

// ParseBrokerSummaryBatch parses the batch broker-summary response
// (POST /stocks/broker-summary/batch) into per-ticker results.
//
// The batch endpoint's `data` is an object keyed by ticker, unlike the
// single-ticker endpoint's flat array. Each ticker is parsed independently: a
// payload that violates a source invariant lands in Batch.Invalid with its
// reason, while the rest of the batch is still usable — a ticker-day is atomic,
// so one bad payload must not throw away the other 49 already-paid ticker-days.
// A batch-level problem (non-JSON, success=false, data not an object) is an
// error and nothing is returned.
func ParseBrokerSummaryBatch(body []byte) (*broker.Batch, error) {
	var env wireEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("%w: decode response: %v", ErrMalformedPayload, err)
	}
	if !env.Success {
		return nil, fmt.Errorf("%w: success=false error=%s", ErrMalformedPayload, rawText(env.Error))
	}

	batch := &broker.Batch{
		Results: make(map[string]*broker.Result),
		Invalid: make(map[string]error),
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return batch, nil
	}

	var byTicker map[string]json.RawMessage
	if err := json.Unmarshal(env.Data, &byTicker); err != nil {
		return nil, fmt.Errorf("%w: data is not a ticker-keyed object: %v", ErrMalformedPayload, err)
	}

	for rawTicker, raw := range byTicker {
		ticker := strings.ToUpper(strings.TrimSpace(rawTicker))
		if ticker == "" {
			continue
		}
		if len(raw) == 0 || string(raw) == "null" {
			// The vendor listed the ticker but returned no rows: an empty day,
			// not a malformed payload.
			batch.Results[ticker] = &broker.Result{Buyers: []broker.Row{}, Sellers: []broker.Row{}}
			continue
		}
		var rows []wireRow
		if err := json.Unmarshal(raw, &rows); err != nil {
			batch.Invalid[ticker] = fmt.Errorf("%w: %s data is not a broker row array: %v", ErrMalformedPayload, ticker, err)
			continue
		}
		res, err := buildResult(ticker, rows)
		if err != nil {
			batch.Invalid[ticker] = err
			continue
		}
		batch.Results[ticker] = res
	}
	return batch, nil
}

// buildResult converts wire rows into buyers, sellers and derived totals.
func buildResult(ticker string, rows []wireRow) (*broker.Result, error) {
	res := &broker.Result{Buyers: []broker.Row{}, Sellers: []broker.Row{}}
	seenBuy := make(map[string]bool, len(rows))
	seenSell := make(map[string]bool, len(rows))
	var buyValue, sellValue, sellLot int64

	for i, w := range rows {
		code := strings.TrimSpace(w.Code)
		if code == "" {
			return nil, fmt.Errorf("%w: %s row %d has no broker code", ErrMalformedPayload, ticker, i)
		}
		if w.BuyVolume < 0 || w.SellVolume < 0 || w.BuyValue < 0 || w.SellValue < 0 || w.BuyAvg < 0 || w.SellAvg < 0 {
			return nil, fmt.Errorf("%w: %s broker %s has negative fields", ErrMalformedPayload, ticker, code)
		}
		if w.BuyVolume%100 != 0 || w.SellVolume%100 != 0 {
			return nil, fmt.Errorf("%w: %s broker %s volume is not a whole lot (%d buy / %d sell shares)",
				ErrMalformedPayload, ticker, code, w.BuyVolume, w.SellVolume)
		}
		if w.BuyVolume == 0 && w.BuyValue != 0 {
			return nil, fmt.Errorf("%w: %s broker %s has buy value without buy volume", ErrMalformedPayload, ticker, code)
		}
		if w.SellVolume == 0 && w.SellValue != 0 {
			return nil, fmt.Errorf("%w: %s broker %s has sell value without sell volume", ErrMalformedPayload, ticker, code)
		}

		if w.BuyVolume > 0 {
			if seenBuy[code] {
				return nil, fmt.Errorf("%w: %s broker %s appears twice on the buy side", ErrMalformedPayload, ticker, code)
			}
			seenBuy[code] = true
			row := broker.Row{
				BrokerCode: code,
				Lot:        w.BuyVolume / 100,
				Value:      roundToInt64(w.BuyValue),
				AvgPrice:   roundToInt64(w.BuyAvg),
			}
			res.Buyers = append(res.Buyers, row)
			buyValue += row.Value
		}
		if w.SellVolume > 0 {
			if seenSell[code] {
				return nil, fmt.Errorf("%w: %s broker %s appears twice on the sell side", ErrMalformedPayload, ticker, code)
			}
			seenSell[code] = true
			row := broker.Row{
				BrokerCode: code,
				Lot:        w.SellVolume / 100,
				Value:      roundToInt64(w.SellValue),
				AvgPrice:   roundToInt64(w.SellAvg),
			}
			res.Sellers = append(res.Sellers, row)
			sellValue += row.Value
			sellLot += row.Lot
		}
	}

	// Every share has a buyer and a seller, so a complete payload reconciles
	// exactly. A mismatch means the response is partial or drifted — refuse it
	// rather than persist half a day.
	if buyValue != sellValue {
		return nil, fmt.Errorf("%w: %s Σbuy=%d ≠ Σsell=%d", ErrMalformedPayload, ticker, buyValue, sellValue)
	}

	rankByValue(res.Buyers)
	rankByValue(res.Sellers)

	res.Totals = broker.Totals{TVal: sellValue, TLot: sellLot}
	if sellLot > 0 {
		res.Totals.Avg = roundToInt64(float64(sellValue) / float64(sellLot*100))
	}
	// FNVal stays nil: the broker endpoint carries no foreign net, and
	// unobserved must never be persisted as zero (ticket 06 fills it).
	return res, nil
}

// rankByValue sorts a side by value desc (broker code breaks ties for
// determinism) and assigns a dense 1..N rank, so the read-time top-10
// projection is correct.
func rankByValue(rows []broker.Row) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Value != rows[j].Value {
			return rows[i].Value > rows[j].Value
		}
		return rows[i].BrokerCode < rows[j].BrokerCode
	})
	for i := range rows {
		rows[i].Rank = i + 1
	}
}

func roundToInt64(v float64) int64 {
	return int64(math.Round(v))
}

// rawText renders a raw JSON field for an error message, without failing when
// the field is null or absent.
func rawText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "<none>"
	}
	return strings.TrimSpace(string(raw))
}
