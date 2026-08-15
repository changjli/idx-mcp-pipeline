package ipot

import "testing"

func TestParseValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
	}{
		{"plain integer", "883", 883},
		{"plain integer with trailing space", "883 ", 883},
		{"comma thousands", "169,544", 169544},
		{"comma thousands with trailing space", "808,975 ", 808975},
		{"zero", "0", 0},
		{"zero with trailing space", "0 ", 0},
		{"billions with space", "15.0 B", 15_000_000_000},
		{"billions no space", "635.4B", 635_400_000_000},
		{"billions decimal", "71.2 B", 71_200_000_000},
		{"trillions", "2.7 T", 2_700_000_000_000},
		{"millions", "4.6 M", 4_600_000},
		{"millions no space", "1.1M", 1_100_000},
		{"thousands", "12.5 K", 12_500},
		{"negative", "-5.0 B", -5_000_000_000},
		{"empty", "", 0},
		{"dash placeholder", "-", 0},
		{"n/a placeholder", "N/A", 0},
		{"fractional billions exact", "8548.3 B", 8_548_300_000_000},
		{"fractional trillions exact", "8519.7 T", 8_519_700_000_000_000},
		{"fractional millions exact", "4.6 M", 4_600_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseValue(tt.input)
			if err != nil {
				t.Fatalf("parseValue(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("parseValue(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseValue_Invalid(t *testing.T) {
	inputs := []string{"abc", "12.3 X", "1.2.3 B", "--5", ".5"}
	for _, in := range inputs {
		if _, err := parseValue(in); err == nil {
			t.Errorf("parseValue(%q) expected error, got nil", in)
		}
	}
}
