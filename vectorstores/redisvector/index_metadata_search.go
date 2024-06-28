package redisvector

import (
	"errors"
	"strconv"
)

func NewIndexMetadataSearch(index string, opts ...SearchOption) (*IndexVectorSearch, error) {
	if index == "" {
		return nil, errors.New("invalid index")
	}
	s := &IndexVectorSearch{
		index:   index,
		returns: []string{},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s IndexVectorSearch) AsMetadataSearchCommand() []string {

	// "FT.SEARCH" "users"
	// "@job:("engineer")"
	// "RETURN" "4" "content" "user" "age"
	// "SORTBY" "distance" "ASC"
	// "DIALECT" "2"
	// "LIMIT" "0" "3"
	cmd := []string{"FT.SEARCH", s.index}

	if s.limit == 0 {
		s.limit = 1
	}

	filter := "*"
	if len(s.preFilters) > 0 {
		filter = s.preFilters
	}
	cmd = append(cmd, filter)

	if l := len(s.returns); l > 0 {
		cmd = append(cmd, "RETURN", strconv.Itoa(len(s.returns)))
		cmd = append(cmd, s.returns...)
	}

	cmd = append(cmd, "DIALECT", "2")
	cmd = append(cmd, "LIMIT", strconv.Itoa(s.offset), strconv.Itoa(s.limit))

	return cmd
}
