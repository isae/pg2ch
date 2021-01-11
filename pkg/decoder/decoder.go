package decoder

// based on https://github.com/kyleconroy/pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils/dbtypes"
)

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

const (
	truncateCascadeBit         = 1
	truncateRestartIdentityBit = 2

	InsertMsgType   = 'I'
	DeleteMsgType   = 'D'
	UpdateMsgType   = 'U'
	TruncateMsgType = 'T'
	BeginMsgType    = 'B'
	CommitMsgType   = 'C'
	TypeMsgType     = 'Y'
	OriginMsgType   = 'O'
	RelationMsgType = 'R'
)

func (d *decoder) bool() bool { return d.buf.Next(1)[0] != 0 }

func (d *decoder) uint8() uint8     { return d.buf.Next(1)[0] }
func (d *decoder) uint16() uint16   { return d.order.Uint16(d.buf.Next(2)) }
func (d *decoder) uint32() uint32   { return d.order.Uint32(d.buf.Next(4)) }
func (d *decoder) uint64() uint64   { return d.order.Uint64(d.buf.Next(8)) }
func (d *decoder) oid() dbtypes.OID { return dbtypes.OID(d.uint32()) }
func (d *decoder) lsn() dbtypes.LSN { return dbtypes.LSN(d.uint64()) }

func (d *decoder) int8() int8   { return int8(d.uint8()) }
func (d *decoder) int16() int16 { return int16(d.uint16()) }
func (d *decoder) int32() int32 { return int32(d.uint32()) }
func (d *decoder) int64() int64 { return int64(d.uint64()) }

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		panic(err)
	}

	return string(s[:len(s)-1])
}

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowInfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	}

	_ = d.buf.UnreadByte()
	return false
}

func (d *decoder) tupleData() []*message.Tuple {
	size := int(d.uint16())
	data := make([]*message.Tuple, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = &message.Tuple{Kind: message.TupleText, Value: d.buf.Next(vsize)}
		case 'n':
			data[i] = &message.Tuple{Kind: message.TupleNull, Value: []byte{}}
		case 'u':
			data[i] = &message.Tuple{Kind: message.TupleUnchanged, Value: []byte{}}
		}
	}

	return data
}

func (d *decoder) columns() []message.Column {
	size := int(d.uint16())
	data := make([]message.Column, size)
	for i := 0; i < size; i++ {
		data[i] = message.Column{}
		data[i].IsKey = d.bool()
		data[i].Name = d.string()
		data[i].TypeOID = d.oid()
		data[i].Mode = d.int32()
	}

	return data
}

// Parse a logical replication message.
// See https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
func Parse(src []byte) (message.Message, error) {
	msgType := src[0]
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}
	switch msgType {
	case BeginMsgType:
		m := &message.Begin{}

		m.FinalLSN = d.lsn()
		m.Timestamp = d.timestamp()
		m.XID = d.int32()

		return m, nil
	case CommitMsgType:
		m := &message.Commit{}

		m.Flags = d.uint8()
		m.LSN = d.lsn()
		m.TransactionLSN = d.lsn()
		m.Timestamp = d.timestamp()

		return m, nil
	case OriginMsgType:
		m := &message.Origin{}

		m.LSN = d.lsn()
		m.Name = d.string()

		return m, nil
	case RelationMsgType:
		m := &message.Relation{}

		m.OID = d.oid()
		m.Namespace = d.string()
		m.Name = d.string()
		m.ReplicaIdentity = message.ReplicaIdentity(d.uint8())
		m.Columns = d.columns()

		return m, nil
	case TypeMsgType:
		m := &message.Type{}

		m.OID = d.oid()
		m.Namespace = d.string()
		m.Name = d.string()

		return m, nil
	case InsertMsgType:
		m := &message.Insert{}

		m.RelationOID = d.oid()
		m.IsNew = d.uint8() == 'N'
		m.NewRow = d.tupleData()

		return m, nil
	case UpdateMsgType:
		m := &message.Update{}

		m.RelationOID = d.oid()
		m.IsKey = d.rowInfo('K')
		m.IsOld = d.rowInfo('O')
		if m.IsKey || m.IsOld {
			m.OldRow = d.tupleData()
		}
		m.IsNew = d.uint8() == 'N'
		m.NewRow = d.tupleData()

		return m, nil
	case DeleteMsgType:
		m := &message.Delete{}

		m.RelationOID = d.oid()
		m.IsKey = d.rowInfo('K')
		m.IsOld = d.rowInfo('O')
		m.OldRow = d.tupleData()

		return m, nil
	case TruncateMsgType:
		m := &message.Truncate{}

		relationsCnt := int(d.uint32())
		options := d.uint8()
		m.Cascade = options&truncateCascadeBit == 1
		m.RestartIdentity = options&truncateRestartIdentityBit == 1

		m.RelationOIDs = make([]dbtypes.OID, relationsCnt)
		for i := 0; i < relationsCnt; i++ {
			m.RelationOIDs[i] = d.oid()
		}

		return m, nil
	default:
		return nil, fmt.Errorf("unknown message type for %s (%d)", []byte{msgType}, msgType)
	}
}
