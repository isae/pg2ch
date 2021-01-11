package tableinfo

import (
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

func TableChColumns(conn *chutils.CHConn, chTable config.ChTableName) (map[string]config.ChColumn, error) {
	result := make(map[string]config.ChColumn)

	rows, err := conn.Query(fmt.Sprintf("select name, type from system.columns where database = '%s' and table = '%s'",
		chTable.DatabaseName, chTable.TableName)) //TODO: fix SQL injections
	if err != nil {
		return nil, fmt.Errorf("could not query: %v", err)
	}

	for _, line := range rows {
		result[line[0]] = config.ChColumn{
			Name:   line[0],
			Column: parseChType(line[1]),
		}
	}

	return result, nil
}
