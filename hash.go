package sqlcache

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/mitchellh/hashstructure/v2"
)

func defaultHashFunc(query string, args []driver.NamedValue) (string, error) {
	u64, err := hashstructure.Hash(struct {
		Query string
		Args  []driver.NamedValue
	}{
		Query: query,
		Args:  args,
	}, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}

	key := fmt.Sprintf("q%da%dh%s", len(query), len(args), strconv.FormatUint(u64, 10))
	return key, nil
}

// NoopHash returns a string representation of the query and args. Whitespaces
// in the query string is stripped off.
func NoopHash(query string, args []driver.NamedValue) (string, error) {
	var b strings.Builder
	b.Grow(len(query) + len(args)*10) // arbitrary
	for _, ch := range query {
		if !unicode.IsSpace(ch) {
			b.WriteRune(ch)
		}
	}
	b.WriteRune(':')
	b.WriteString(fmt.Sprintf("%v", args))

	return b.String(), nil
}
