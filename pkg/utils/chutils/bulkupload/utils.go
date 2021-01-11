package bulkupload

import (
	"bytes"
	"fmt"

	"github.com/mkabilov/pg2ch/pkg/utils/chutils"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

func decodeDigit(c byte, onlyOctal bool) (byte, bool) {
	switch {
	case c >= '0' && c <= '7':
		return c - '0', true
	case !onlyOctal && c >= '8' && c <= '9':
		return c - '0', true
	case !onlyOctal && c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case !onlyOctal && c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func decodeOctDigit(c byte) (byte, bool) { return decodeDigit(c, true) }
func decodeHexDigit(c byte) (byte, bool) { return decodeDigit(c, false) }

// DecodeCopyToTuples decodes line from the text pg copy command into a array of tuples
func DecodeCopyToTuples(colBuf *bytes.Buffer, out utils.Writer, pgColumns map[int]*config.PgColumn, colProps map[int]*config.ColumnProperty, in []byte) error {
	colBuf.Reset()
	colId := 0

	tupleKind := message.TupleText
	for i, n := 0, len(in); i < n; i++ {
		if in[i] == '\t' || in[i] == '\n' {
			pgCol, processColumn := pgColumns[colId]
			if colId > 0 && processColumn {
				if err := out.WriteByte('\t'); err != nil {
					return err
				}
			}
			colProp := colProps[colId]
			tupleData := &message.Tuple{Kind: tupleKind, Value: colBuf.Bytes()}

			if in[i] == '\t' {
				if processColumn {
					if err := chutils.ConvertColumn(out, pgCol, tupleData, colProp); err != nil {
						return err
					}
				}

				tupleKind = message.TupleText
				colBuf.Reset()
				colId++
				continue
			} else if in[i] == '\n' {
				if processColumn {
					if err := chutils.ConvertColumn(out, pgCol, tupleData, colProp); err != nil {
						return err
					}
				}

				return nil
			}
		} else if in[i] != '\\' {
			colBuf.WriteByte(in[i])
			continue
		}
		i++
		if i >= n {
			return fmt.Errorf("unknown escape sequence: %q", in[i-1:])
		}

		ch := in[i]
		if ch == 'N' {
			tupleKind = message.TupleNull
			continue
		}

		switch ch {
		case 'b':
			colBuf.Write([]byte(`\b`))
			continue
		case 'f':
			colBuf.Write([]byte(`\f`))
			continue
		case 'n':
			colBuf.Write([]byte(`\n`))
			continue
		case 'r':
			colBuf.Write([]byte(`\r`))
			continue
		case 't':
			colBuf.Write([]byte(`\t`))
			continue
		case 'v':
			colBuf.Write([]byte(`\v`))
			continue
		case '\\':
			colBuf.Write([]byte(`\\`))
			continue
		}

		if ch == 'x' {
			// \x can be followed by 1 or 2 hex digits.
			i++
			if i >= n {
				return fmt.Errorf("unknown escape sequence: %q", in[i-2:])
			}

			ch = in[i]
			digit, ok := decodeHexDigit(ch)
			if !ok {
				return fmt.Errorf("unknown escape sequence: %q", in[i-2:i])
			}
			if i+1 < n {
				if v, ok := decodeHexDigit(in[i+1]); ok {
					i++
					digit <<= 4
					digit += v
				}
			}
			colBuf.WriteByte(digit)

			continue
		}

		if ch >= '0' && ch <= '7' {
			digit, _ := decodeOctDigit(ch)
			// 1 to 2 more octal digits follow.
			if i+1 < n {
				if v, ok := decodeOctDigit(in[i+1]); ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			if i+1 < n {
				if v, ok := decodeOctDigit(in[i+1]); ok {
					i++
					digit <<= 3
					digit += v
				}
			}
			colBuf.WriteByte(digit)

			continue
		}

		return fmt.Errorf("unknown escape sequence: %q", in[i-1:i+1])
	}

	return fmt.Errorf("should not reach")
}
