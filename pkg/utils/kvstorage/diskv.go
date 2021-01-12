package kvstorage

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/peterbourgon/diskv"
	"pg2ch/pkg/utils/dbtypes"
)

type diskvStorage struct {
	location string
	storage  *diskv.Diskv
}

func newDiskvStorage(location string) (KVStorage, error) {
	storage := diskv.New(diskv.Options{
		BasePath:     location,
		CacheSizeMax: 100 * 1024 * 1024, // 100MB
	})
	return &diskvStorage{storage: storage}, nil
}

func init() {
	Register("diskv", newDiskvStorage)
}

func (s *diskvStorage) Has(key string) bool {
	return s.storage.Has(key)
}

func (s *diskvStorage) ReadLSN(key string) (dbtypes.LSN, error) {
	var lsn dbtypes.LSN

	val, err := s.storage.Read(key)
	if err != nil {
		return dbtypes.InvalidLSN, err
	}
	err = lsn.Parse(string(val))
	if err != nil {
		panic(fmt.Sprintf("corrupted lsn in storage: %s, %v", lsn, err))
	}
	return lsn, nil
}

func (s *diskvStorage) WriteLSN(key string, lsn dbtypes.LSN) error {
	return s.storage.WriteStream(key, bytes.NewReader(lsn.FormattedBytes()), true)
}

func (s *diskvStorage) ReadUint(key string) (uint64, error) {
	cacheData, err := s.storage.Read(key)
	if err != nil {
		return 0, err
	}
	val, err2 := strconv.ParseUint(string(cacheData), 10, 64)
	if err2 != nil {
		return 0, err
	}
	return val, nil
}

func (s *diskvStorage) WriteUint(key string, val uint64) error {
	return s.storage.Write(key, []byte(fmt.Sprintf("%v", val)))
}

func (s *diskvStorage) Keys() []string {
	var result []string
	for key := range s.storage.Keys(nil) {
		result = append(result, key)
	}
	return result
}

func (s *diskvStorage) Erase(key string) error {
	return s.storage.Erase(key)
}

func (s *diskvStorage) Close() error {
	s.storage = nil
	return nil
}
