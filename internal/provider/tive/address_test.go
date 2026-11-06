package tive

import "testing"

// mapping.md "Address parsing (conservative)": the tail is interpreted; whatever precedes the locality is the street.
func TestParseAddress(t *testing.T) {
	s := func(v string) *string { return &v }
	tests := []struct {
		name                                     string
		in                                       string
		street, locality, state, country, postal *string
	}{
		{"US with street", "114 Hunts Point Market, Bronx, NY 10474, USA", s("114 Hunts Point Market"), s("Bronx"), s("NY"), s("USA"), s("10474")},
		{"US street address", "1460 E Hi Line Rd, Pharr, TX 78577, USA", s("1460 E Hi Line Rd"), s("Pharr"), s("TX"), s("USA"), s("78577")},
		{"street with a comma in it", "Suite 5, 12 Main St, Austin, TX 78701, USA", s("Suite 5, 12 Main St"), s("Austin"), s("TX"), s("USA"), s("78701")},
		{"US city only", "Boston, MA 02101, USA", nil, s("Boston"), s("MA"), s("USA"), s("02101")},
		{"US zip+4", "Austin, TX 78701-1234, USA", nil, s("Austin"), s("TX"), s("USA"), s("78701-1234")},
		{"city and country", "London, UK", nil, s("London"), nil, s("UK"), nil},
		{"two part state zip", "Boston, MA 02101", nil, s("Boston"), s("MA"), nil, s("02101")},
		{"single token", "Somewhere", nil, nil, nil, nil, nil},
		{"empty", "", nil, nil, nil, nil, nil},
		{"unmatched middle", "A, B, C, D", nil, nil, nil, nil, nil},
		{"lowercase state", "x, bronx, ny 10474, USA", nil, nil, nil, nil, nil},
		{"digits in country", "A, B, NY 10474, 123", nil, nil, nil, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := parseAddress(tt.in)
			for field, pair := range map[string][2]*string{
				"street": {a.Street, tt.street}, "locality": {a.Locality, tt.locality}, "state": {a.State, tt.state},
				"country": {a.Country, tt.country}, "postal": {a.PostalCode, tt.postal},
			} {
				got, want := pair[0], pair[1]
				if (got == nil) != (want == nil) || (got != nil && *got != *want) {
					t.Fatalf("%s = %v, want %v", field, deref(got), deref(want))
				}
			}
			if tt.in != "" && (a.FullAddress == nil || *a.FullAddress != tt.in) {
				t.Fatal("full_address must be the verbatim input")
			}
		})
	}
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}
