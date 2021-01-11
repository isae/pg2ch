package tests

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ildus/pqt"
	"github.com/stretchr/testify/assert"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/replicator"
	"github.com/mkabilov/pg2ch/pkg/utils/chutils"
)

const (
	initPg = `
do $$
begin
	if not exists (select from pg_catalog.pg_roles where rolname = 'postgres')
	then
		create role postgres superuser login;
	end if;
end $$;

create extension istore;

create table pg1(id bigserial, a int, b int, c bigint, d text, f1 float,
	f2 double precision, bo bool, num numeric(10, 2), ch varchar(10));
alter table pg1 replica identity full;

create table pg2(id bigserial, a int[], b bigint[], c text[]);
alter table pg2 replica identity full;

create table pg3(id bigserial, a istore, b bigistore);
alter table pg3 replica identity full;

insert into pg1(a,b,c,d,f1,f2,bo,num,ch) select i, i + 1, i + 2, i::text, i + 1.1,
	i + 2.1, true, i + 3, (i+4)::text
from generate_series(1, 10000) i;

insert into pg2(a, b, c) select array_fill(i, array[3]), array_fill(i + 1, array[3]),
	array_fill(i::text, array[3])
from generate_series(1, 10000) i;

insert into pg3(a, b) select
	istore(array_fill(i, array[3]), array_fill(i + 1, array[3])),
	bigistore(array_fill(i + 2, array[3]), array_fill(i + 3, array[3]))
from generate_series(1, 10000) i;
`
	addSQL = `
insert into pg1(a,b,c,d,f1,f2,bo,num,ch) select i, i + 1, i + 2, i::text,
	i + 1.1, i + 2.1, true, i + 3, E'\t' || (i+4)::text from generate_series(1, %[1]d) i;

insert into pg2(a, b, c) select array_fill(i, array[3]), array_fill(i + 1, array[3]),
	array_fill(i::text, array[3]) from generate_series(1, %[1]d) i;

insert into pg3(a, b) select
	istore(array_fill(i, array[1]), array_fill(i + 1, array[1])),
	bigistore(array_fill(i + 2, array[1]), array_fill(i + 3, array[1]))
from generate_series(1, %[1]d) i;
`
	testConfigFile = "./test.yaml"
)

var (
	addsql100    = fmt.Sprintf(addSQL, 100)
	addsql100000 = fmt.Sprintf(addSQL, 100000)
)

type CHLink struct {
	conn *chutils.CHConn
}

var (
	ch     CHLink
	initch = []string{
		"drop database if exists pg2ch_test;",
		"create database pg2ch_test;",
		`create table pg2ch_test.ch1(
			id UInt64,
			a Int32,
			b Int32,
			c Int64,
			d String,
			f1 Float32,
			f2 Float64,
			bo Int8,
			num Decimal(10, 2),
			ch String,
			sign Int8,
		    lsn UInt64,
			table_name LowCardinality(String) CODEC(Delta, LZ4)
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch1_aux(
			id UInt64,
			a Int32,
			b Int32,
			c Int64,
			d String,
			f1 Float32,
			f2 Float64,
			bo Int8,
			num Decimal(10, 2),
			ch String,
			sign Int8,
			row_id UInt64,
			lsn UInt64,
			table_name String
		) engine=MergeTree() order by lsn partition by (table_name);`,
		`create table pg2ch_test.ch2(
			id UInt64,
			a Array(Int32),
			b Array(Int64),
			c Array(String),
			sign Int8,
			lsn UInt64,
			table_name LowCardinality(String) CODEC(Delta, LZ4)
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch2_aux(
			id UInt64,
			a Array(Int32),
			b Array(Int64),
			c Array(String),
			sign Int8,
			row_id UInt64,
			lsn UInt64,
			table_name String
		) engine=MergeTree() order by lsn partition by (table_name);`,
		`create table pg2ch_test.ch3(
			id UInt64,
			a_keys Array(Int32),
			a_values Array(Int32),
			b_keys Array(Int32),
			b_values Array(Int64),
			sign Int8,
			lsn UInt64,
			table_name LowCardinality(String) CODEC(Delta, LZ4)
		 ) engine=CollapsingMergeTree(sign) order by id;`,
		`create table pg2ch_test.ch3_aux(
			id UInt64,
			a_keys Array(Int32),
			a_values Array(Int32),
			b_keys Array(Int32),
			b_values Array(Int64),
			sign Int8,
			row_id UInt64,
			lsn UInt64,
			table_name String
		) engine=MergeTree() order by lsn partition by (table_name);`,
	}
)

func (ch *CHLink) safeExec(t *testing.T, sql string) {
	err := ch.conn.Exec(sql)
	if err != nil {
		t.Fatal("could not exec query:", err)
	}
}

func (ch *CHLink) safeQuery(t *testing.T, sql string) [][]string {
	rows, err := ch.conn.Query(sql)
	if err != nil {
		t.Fatal("could not make query:", err)
	}
	return rows
}

func (ch *CHLink) waitForCount(t *testing.T, query string, minCount int, maxAttempts int) {
	attempt := 0

	for {
		rows := ch.safeQuery(t, query)
		recCount, err := strconv.Atoi(rows[0][0])
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("query %q, attempt: %v, count: %v (min count: %v)", query, attempt, recCount, minCount)

		if recCount >= minCount {
			break
		}

		attempt += 1
		time.Sleep(time.Second * 2)

		if attempt >= maxAttempts {
			t.Fatalf("attempts exceeded for query: %v", query)
		}
	}

}

func (ch *CHLink) getCount(t *testing.T, query string) int {
	rows := ch.safeQuery(t, query)
	recCount, err := strconv.Atoi(rows[0][0])
	if err != nil {
		t.Fatalf("count not get count for: %s : %s", query, err)
	}

	return recCount
}

func initNode(t *testing.T) (*pqt.PostgresNode, *config.Config) {
	node := pqt.MakePostgresNode("master")

	config.DefaultPostgresPort = uint16(node.Port)
	config.DefaultInactivityMergeTimeout = time.Second

	cfg, err := config.New(testConfigFile)
	if cfg.PersStorageType == "diskv" {
		dbPath, err := ioutil.TempDir("", "pg2ch_diskv_dat")
		if err != nil {
			log.Fatal(err)
		}

		cfg.PersStoragePath = dbPath
	} else if cfg.PersStorageType == "mmap" {
		tmpfile, err := ioutil.TempFile("", "pg2ch_mmap_dat")
		fmt.Println(tmpfile.Name())
		if err != nil {
			t.Fatal(err)
		}
		cfg.PersStoragePath = tmpfile.Name()
	} else {
		t.Fatal("unknown db type")
	}
	ch.conn = chutils.MakeChConnection(&cfg.ClickHouse, cfg.GzipCompression.UseCompression())

	if err != nil {
		log.Fatal("config parsing error: ", err)
	}

	node.Init()
	node.AppendConf("postgresql.conf", `
log_min_messages = ERROR
log_statement = none
hot_standby = on
wal_keep_segments = 10
wal_level = logical
max_logical_replication_workers = 10
`)

	node.AppendConf("pg_hba.conf", `
	   local	all		all						trust
	   host	all		all		127.0.0.1/32	trust
	   host	all		all		::1/128			trust
	   local	replication		all						trust
	   host	replication		all		127.0.0.1/32	trust
	   host	replication		all		::1/128			trust
	`)
	node.Start()
	node.Execute("postgres", initPg)
	node.Execute("postgres", fmt.Sprintf("create publication %s for all tables",
		cfg.Postgres.PublicationName))
	node.Execute("postgres", fmt.Sprintf("select pg_create_logical_replication_slot('%s', 'pgoutput')",
		cfg.Postgres.ReplicationSlotName))

	for _, s := range initch {
		ch.safeExec(t, s)
	}

	return node, cfg
}

func TestBasicSync(t *testing.T) {
	node, cfg := initNode(t)
	defer node.Stop()
	defer os.RemoveAll(cfg.PersStoragePath)

	var repl *replicator.Replicator

	repl = replicator.New(cfg)
	stopCh := make(chan bool, 1)

	go func() {
		err := repl.Run()
		if err != nil {
			stopCh <- true
			t.Fatal("could not start replicator: ", err)
		}
		repl.PrintTablesLSN()
		stopCh <- true
	}()

	t.Run("sync and first data", func(t *testing.T) {
		defer repl.Finish()

		ch.waitForCount(t, "select count(*) from pg2ch_test.ch1", 1, 10)
		if count := ch.getCount(t, "select count(*) from pg2ch_test.ch1"); count != 10000 {
			t.Fatalf("count for ch1 should be equal to 10000, got: %v", count)
		}

		for i := 0; i < 100; i++ {
			node.Execute("postgres", addsql100)
		}
		ch.waitForCount(t, "select count(*) from pg2ch_test.ch1", 20000, 10)
		if count := ch.getCount(t, "select count(*) from pg2ch_test.ch1"); count != 20000 {
			t.Fatalf("count for ch1 should be equal to 20000, got: %v", count)
		}

		if count := ch.getCount(t, "select count(*) from pg2ch_test.ch2"); count != 20000 {
			t.Fatalf("count for ch2 should be equal to 20000, got: %v", count)
		}

		if count := ch.getCount(t, "select count(*) from pg2ch_test.ch3"); count != 20000 {
			t.Fatalf("ch3: count should be equal to 20000, got: %v", count)
		}

		rows := ch.safeQuery(t, "select * from pg2ch_test.ch1 order by id desc limit 10")
		assert.Equal(t, []string{"20000", "100", "101", "102", "100", "101.1", "102.1", "1", "103.00", "\\t104", "1"}, rows[0][0:len(rows[0])-2], "row 0")

		rows = ch.safeQuery(t, "select * from pg2ch_test.ch2 order by id desc limit 10")
		assert.Equal(t, "20000", rows[0][0], "row 0")
		assert.Equal(t, "[100,100,100]", rows[0][1], "row 0")
		assert.Equal(t, "[101,101,101]", rows[0][2], "row 0")
		assert.Equal(t, "['100','100','100']", rows[0][3], "row 0")
		assert.Equal(t, "1", rows[0][4], "row 0")

		assert.Equal(t, "19999", rows[1][0], "row 1")
		assert.Equal(t, "[99,99,99]", rows[1][1], "row 1")
		assert.Equal(t, "[100,100,100]", rows[1][2], "row 1")
		assert.Equal(t, "['99','99','99']", rows[1][3], "row 1")
		assert.Equal(t, "1", rows[1][4], "row 0")

		rows = ch.safeQuery(t, "select * from pg2ch_test.ch3 order by id desc limit 10")
		assert.Equal(t, []string{"20000", "[100]", "[101]", "[102]", "[103]", "1"}, rows[0][0:len(rows[0])-2])
	})

	<-stopCh
	repl = replicator.New(cfg)

	go func() {
		err := repl.Run()
		if err != nil {
			stopCh <- true
			t.Fatal("could not start replicator: ", err)
		}
		repl.PrintTablesLSN()
		stopCh <- true
	}()

	t.Run("second round of data", func(t *testing.T) {
		defer repl.Finish()

		for repl.State() != replicator.StateWorking {
			time.Sleep(time.Second)
		}
		repl.PrintTablesLSN()

		count := ch.getCount(t, "select count(*) from pg2ch_test.ch1")
		assert.Equal(t, 20000, count, "expected right count in ch1")

		for i := 0; i < 100; i++ {
			node.Execute("postgres", addsql100)
		}

		ch.waitForCount(t, "select count(*) from pg2ch_test.ch1", 30000, 10)
		count = ch.getCount(t, "select count(*) from pg2ch_test.ch1")
		assert.Equal(t, 30000, count, "expected right count in ch1")

		ch.waitForCount(t, "select count(*) from pg2ch_test.ch2", 30000, 10)
		count = ch.getCount(t, "select count(*) from pg2ch_test.ch2")
		assert.Equal(t, 30000, count, "expected right count in ch2")

		ch.waitForCount(t, "select count(*) from pg2ch_test.ch3", 30000, 10)
		count = ch.getCount(t, "select count(*) from pg2ch_test.ch3")
		assert.Equal(t, 30000, count, "expected right count in ch3")
	})

	<-stopCh
}

func TestConcurrentSync(t *testing.T) {
	var repl *replicator.Replicator

	node, cfg := initNode(t)
	defer node.Stop()
	defer os.RemoveAll(cfg.PersStoragePath)

	expected := 10000 // from initPg

	repl = replicator.New(cfg)
	stopCh := make(chan bool, 1)

	expected += 5 * 100000
	for i := 0; i < 5; i++ {
		node.Execute("postgres", addsql100000)
	}

	expected += 10 * 100
	/* we're starting to add values before sync */
	go func() {
		for i := 0; i < 10; i++ {
			node.Execute("postgres", addsql100)
			time.Sleep(time.Second)
		}
	}()

	expected += 10 * 100
	go func() {
		for i := 0; i < 10; i++ {
			node.Execute("postgres", addsql100)
			time.Sleep(time.Second)
		}
	}()

	go func() {
		err := repl.Run()
		if err != nil {
			stopCh <- true
			t.Fatal("could not start replicator: ", err)
		}
		stopCh <- true
	}()

	t.Run("checking concurrent inserted data", func(t *testing.T) {
		defer repl.Finish()

		for repl.State() != replicator.StateWorking {
			time.Sleep(time.Second)
		}

		ch.waitForCount(t, "select count(*) from pg2ch_test.ch1", expected, 100)
		ch.waitForCount(t, "select count(*) from pg2ch_test.ch2", expected, 100)
		ch.waitForCount(t, "select count(*) from pg2ch_test.ch3", expected, 100)

		count := ch.getCount(t, "select count(*) from pg2ch_test.ch1")
		assert.Equal(t, expected, count, "expected right count in ch1")

		count = ch.getCount(t, "select count(*) from pg2ch_test.ch2")
		assert.Equal(t, expected, count, "expected right count in ch2")

		count = ch.getCount(t, "select count(*) from pg2ch_test.ch3")
		assert.Equal(t, expected, count, "expected right count in ch3")
	})

	<-stopCh
}
