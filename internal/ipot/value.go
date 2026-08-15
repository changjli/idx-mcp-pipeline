package ipot

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// suffixMultiplier maps the IPOT value shorthand suffixes to their multiplier.
// IPOT abbreviates large rupiah values: "15.0 B" = 15 billion, "2.7 T" = 2.7
// trillion, "4.6 M" = 4.6 million. Plain numbers (with comma thousands) carry
// no suffix.
var suffixMultiplier = map[string]int64{
	"K": 1_000,
	"M": 1_000_000,
	"B": 1_000_000_000,
	"T": 1_000_000_000_000,
}

// parseValue converts an IPOT numeric cell to an integer rupiah/lot value.
// Handles comma thousands ("169,544"), trailing whitespace ("883 "), and the
// "N.N B" / "N.N T" / "N.N M" billions shorthand. Empty cells, "-", and "N/A"
// (placeholders IPOT uses for a broker with no activity) parse to 0.
//
// The decimal is parsed with exact integer arithmetic (integer part × suffix
// multiplier + fractional part × multiplier / 10^digits) rather than float64,
// so values like "8548.3 B" land exactly on 8,548,300,000,000 instead of
// drifting by float rounding.
func parseValue(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" || strings.EqualFold(s, "N/A") {
		return 0, nil
	}

	mult := int64(1)
	numPart := s
	if last := s[len(s)-1]; last >= 'A' && last <= 'Z' {
		m, ok := suffixMultiplier[string(last)]
		if !ok {
			return 0, fmt.Errorf("unknown suffix %q in %q", string(last), s)
		}
		mult = m
		numPart = s[:len(s)-1]
	}
	numPart = strings.ReplaceAll(numPart, ",", "")
	numPart = strings.TrimSpace(numPart)

	intStr, fracStr, hasFrac := strings.Cut(numPart, ".")
	if intStr == "" {
		return 0, fmt.Errorf("parse number %q: no integer part", numPart)
	}
	intPart, err := strconv.ParseInt(intStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse number %q: %w", numPart, err)
	}
	value := intPart * mult

	if hasFrac {
		fracStr = strings.TrimRight(fracStr, "0")
		if fracStr != "" {
			fracPart, err := strconv.ParseInt(fracStr, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse number %q: %w", numPart, err)
			}
			// Exact: fracPart × mult / 10^len(fracStr). mult is a power of ten,
			// so this is integer arithmetic for any realistic fraction length.
			// Guard the multiplication against overflow for absurd inputs.
			if fracPart > math.MaxInt64/mult {
				return 0, fmt.Errorf("fractional part overflows: %q", s)
			}
			scale := int64(1)
			for range fracStr {
				scale *= 10
			}
			value += fracPart * mult / scale
		}
	}
	return value, nil
}
