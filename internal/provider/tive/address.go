package tive

import (
	"regexp"
	"strings"

	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
)

var (
	stateZipRe = regexp.MustCompile(`^([A-Z]{2}) (\d{5}(?:-\d{4})?)$`)
	countryRe  = regexp.MustCompile(`^[A-Za-z][A-Za-z .]*$`)
)

// parseAddress splits a Tive FormattedAddress conservatively (contracts/mapping.md).
// Only the tail "<locality>, <ST> <postal>, <country>" (or "<locality>, <country>") is
// interpreted; anything unmatched stays nil. Whatever precedes the locality is the street (nil when nothing does),
// and full_address is verbatim.
func parseAddress(full string) *paxafe.Address {
	a := &paxafe.Address{FullAddress: &full}
	parts := strings.Split(full, ", ")
	switch {
	case len(parts) >= 3:
		country, mid, locality := parts[len(parts)-1], parts[len(parts)-2], parts[len(parts)-3]
		m := stateZipRe.FindStringSubmatch(mid)
		if m == nil || !countryRe.MatchString(country) || locality == "" {
			return a
		}
		a.Locality, a.State, a.PostalCode, a.Country = &locality, &m[1], &m[2], &country
		if len(parts) > 3 {
			street := strings.Join(parts[:len(parts)-3], ", ")
			a.Street = &street
		}
	case len(parts) == 2:
		locality, second := parts[0], parts[1]
		if m := stateZipRe.FindStringSubmatch(second); m != nil {
			a.Locality, a.State, a.PostalCode = &locality, &m[1], &m[2]
		} else if countryRe.MatchString(second) && locality != "" {
			a.Locality, a.Country = &locality, &second
		}
	}
	return a
}
