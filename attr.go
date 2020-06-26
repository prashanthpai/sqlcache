package sqlcache

import (
	"regexp"
	"strconv"
)

var (
	attrRegexp = regexp.MustCompile(`(@cache-ttl|@cache-max-rows) (\d+)`)
)

type attributes struct {
	ttl     int
	maxRows int
}

func getAttrs(query string) *attributes {
	matches := attrRegexp.FindAllStringSubmatch(query, 2)
	if len(matches) != 2 {
		return nil
	}

	var attrs attributes
	for _, match := range matches {
		if len(match) != 3 {
			return nil
		}
		switch match[1] {
		case "@cache-ttl":
			ttl, _ := strconv.Atoi(match[2])
			attrs.ttl = ttl
		case "@cache-max-rows":
			maxRows, _ := strconv.Atoi(match[2])
			attrs.maxRows = maxRows
		}
	}

	return &attrs
}
