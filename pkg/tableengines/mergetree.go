package tableengines

import (
	"github.com/jackc/pgx"
	"pg2ch/pkg/config"
	"pg2ch/pkg/message"
	"pg2ch/pkg/utils/chutils/bulkupload"
	"pg2ch/pkg/utils/dbtypes"
)

type mergeTreeTable struct {
	genericTable
}

// NewMergeTree instantiates mergeTreeTable
func NewMergeTree(table genericTable, _ *config.Table) *mergeTreeTable {
	return &mergeTreeTable{
		genericTable: table,
	}
}

// Sync performs initial sync of the data; pgTx is a transaction in which temporary replication slot is created
func (t *mergeTreeTable) Sync(chUploader bulkupload.BulkUploader, pgTx *pgx.Tx, snapshotLSN dbtypes.LSN) error {
	return t.genSync(chUploader, pgTx, snapshotLSN, t)
}

// Write implements io.Writer which is used during the Sync process, see genSync method
func (t *mergeTreeTable) Write(p []byte) (int, error) {
	if err := t.genSyncWrite(p); err != nil {
		return 0, err
	}

	t.printSyncProgress()

	return len(p), nil
}

// Insert handles incoming insert DML operation
func (t *mergeTreeTable) Insert(new message.Row) error {
	return t.writeRow(chTuple{new, nil})
}

// Update handles incoming update DML operation
func (t *mergeTreeTable) Update(old, new message.Row) error {
	return t.writeRow()
}

// Delete handles incoming delete DML operation
func (t *mergeTreeTable) Delete(old message.Row) error {
	return t.writeRow()
}
