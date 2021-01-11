package replicator

import (
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx"
	"gopkg.in/djherbis/buffer.v1"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils/bulkupload"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
	"github.com/mkabilov/pg2ch/pkg/utils/pgutils"
)

func (r *Replicator) SyncTables(syncTables []config.PgTableName, async bool) error {
	if len(syncTables) == 0 {
		r.logger.Info("no tables to sync")
		return nil
	} else {
		r.logger.Infof("%d tables to sync", len(syncTables))
	}

	doneCh := make(chan struct{}, r.cfg.SyncWorkers)
	for i := 0; i < r.cfg.SyncWorkers; i++ {
		go r.syncJob(i, doneCh)
	}

	for _, tblName := range syncTables {
		r.syncJobs <- tblName
	}
	close(r.syncJobs)

	if async {
		go func() {
			for i := 0; i < r.cfg.SyncWorkers; i++ {
				<-doneCh
			}

			r.logger.Infof("sync is finished")
		}()
	} else {
		for i := 0; i < r.cfg.SyncWorkers; i++ {
			<-doneCh
		}
		r.logger.Infof("sync is finished")
	}

	return nil
}

func (r *Replicator) syncTable(chUploader bulkupload.BulkUploader, conn *pgx.Conn, pgTableName config.PgTableName) error {
	if err := chUploader.Init(buffer.New(r.cfg.PipeBufferSize)); err != nil {
		return fmt.Errorf("could not init bulkuploader: %v", err)
	}

	tx, snapshotLSN, err := r.getTxAndLSN(conn, pgTableName)
	if err != nil {
		return err
	}
	defer func() {
		r.logger.Debugf("committing pg transaction, pid: %v", conn.PID())
		if err := tx.Commit(); err != nil {
			r.logger.Infof("could not commit: %v", err)
		}
	}()

	if err := r.chTables[pgTableName].Sync(chUploader, tx, snapshotLSN); err != nil {
		return fmt.Errorf("could not sync: %v", err)
	}

	return nil
}

func (r *Replicator) GetTablesToSync() ([]config.PgTableName, error) {
	var err error
	syncTables := make([]config.PgTableName, 0)

	if err := r.pgDeltaConnect(); err != nil {
		return nil, fmt.Errorf("could not connect: %v", err)
	}
	defer r.pgDeltaDisconnect()

	tx, err := r.pgBegin(r.pgDeltaConn)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %v", err)
	}
	defer r.pgCommit(tx)

	rowsCnt := make(map[config.PgTableName]uint64)
	for tblName := range r.cfg.Tables {
		if r.cfg.Tables[tblName].InitSyncSkip {
			continue
		}

		if r.persStorage.Has(tblName.KeyName()) {
			lsn, _ := r.persStorage.ReadLSN(tblName.KeyName())
			if lsn != dbtypes.InvalidLSN {
				continue
			}
		}

		syncTables = append(syncTables, tblName)

		rowsCnt[tblName], err = pgutils.PgStatLiveTuples(tx, tblName)
		if err != nil {
			return nil, fmt.Errorf("could not get stat live tuples: %v", err)
		}
	}

	if len(syncTables) > 0 {
		sort.SliceStable(syncTables, func(i, j int) bool {
			return rowsCnt[syncTables[i]] > rowsCnt[syncTables[j]]
		})
	}

	r.logger.Infof("need to sync %d tables", len(syncTables))
	for _, pgTableName := range syncTables {
		if err := r.chTables[pgTableName].InitSync(); err != nil {
			return nil, fmt.Errorf("could not start sync for %q table: %v", pgTableName.String(), err)
		}
	}

	return syncTables, nil
}

func (r *Replicator) getTxAndLSN(conn *pgx.Conn, pgTableName config.PgTableName) (*pgx.Tx, dbtypes.LSN, error) { //TODO: better name: getSnapshot?
	for attempt := 0; attempt < r.cfg.CreateSlotMaxAttempts; attempt++ {
		select {
		case <-r.ctx.Done():
			return nil, dbtypes.InvalidLSN, fmt.Errorf("context done")
		default:
		}

		tx, err := r.pgBegin(conn)
		if err != nil {
			r.logger.Warnf("could not begin transaction: %v", err)
			r.pgCommit(tx)
			continue
		}

		tmpSlotName := tempSlotName(pgTableName)
		r.logger.Debugf("creating %q temporary logical replication slot for %q pg table (attempt: %d)",
			tmpSlotName, pgTableName, attempt)

		lsn, err := r.pgCreateTempRepSlot(tx, tmpSlotName)
		if err == nil {
			return tx, lsn, nil
		}

		r.pgRollback(tx)
		r.logger.Warnf("could not create logical replication slot: %v", err)
	}

	return nil, dbtypes.InvalidLSN, fmt.Errorf("attempts exceeded")
}

// go routine
func (r *Replicator) syncJob(jobID int, doneCh chan<- struct{}) {
	defer func() {
		r.logger.Infof("sync job %d finished", jobID)
		doneCh <- struct{}{}
	}()

	conn, err := pgx.Connect(r.pgxConnConfig)
	if err != nil {
		select {
		case r.errCh <- fmt.Errorf("could not connect: %v", err):
		default:
		}
	}
	r.logger.Infof("sync job %d: connected to postgres, pid: %v", jobID, conn.PID())

	defer func() {
		r.logger.Infof("sync job %d: closing pg connection, pid: %v", jobID, conn.PID())
		if err := conn.Close(); err != nil {
			select {
			case r.errCh <- fmt.Errorf("could not close connection: %v", err):
			default:
			}
		}
	}()

	connInfo, err := initPostgresql(conn)
	if err != nil {
		select {
		case r.errCh <- fmt.Errorf("could not fetch conn info: %v", err):
		default:
		}
	}
	conn.ConnInfo = connInfo
	chConn := chutils.MakeChConnection(&r.cfg.ClickHouse, r.cfg.GzipCompression.UseCompression())
	chUploader := bulkupload.New(chConn, r.cfg.GzipBufSize, r.cfg.GzipCompression)
	for pgTableName := range r.syncJobs {
		r.logger.Infof("sync job %d: starting syncing %q pg table", jobID, pgTableName)
		if err := r.syncTable(chUploader, conn, pgTableName); err != nil {
			select {
			case r.errCh <- fmt.Errorf("could not sync table %s: %v", pgTableName, err):
			default:
			}

			return
		}
		if r.syncSleep.Load() > 0 {
			r.logger.Infof("sync job %d: sleeping for %d sec", jobID, r.syncSleep.Load())
			sleepTicker := time.Tick(time.Duration(r.syncSleep.Load()) * time.Second)
			select {
			case <-sleepTicker:
			case <-r.ctx.Done():
			}
		}

		r.logger.Infof("sync job %d: %q table synced", jobID, pgTableName)
	}
}
