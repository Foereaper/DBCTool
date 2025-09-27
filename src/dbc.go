// Copyright (c) 2025 DBCTool
//
// DBCTool is licensed under the MIT License.
// See the LICENSE file for details.

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
    "path/filepath"
)

type DBCHeader struct {
	Magic           [4]byte
	RecordCount     uint32
	FieldCount      uint32
	RecordSize      uint32
	StringBlockSize uint32
}

type SortField struct {
	Name      string `json:"name"`
	Direction string `json:"direction"` // "ASC" or "DESC"
}

type FieldMeta struct {
	Name  string `json:"name"`
	Type  string `json:"type"` // int32, uint32, float, string, Loc
    Count uint32 `json:"count,omitempty"`
}

type MetaFile struct {
    File        string      `json:"file"`
    TableName   string      `json:"tableName,omitempty"`
    PrimaryKeys []string    `json:"primaryKeys"`
    UniqueKeys  [][]string  `json:"uniqueKeys,omitempty"` // array of unique key sets
    SortOrder   []SortField `json:"sortOrder,omitempty"`
    Fields      []FieldMeta `json:"fields"`
}

type Record map[string]interface{}

type DBCFile struct {
	Header      DBCHeader
	Records     []Record
	StringBlock []byte
}

// LoadMeta reads and parses the meta JSON
func LoadMeta(path string) (MetaFile, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return MetaFile{}, fmt.Errorf("failed to read meta file %s: %w", path, err)
    }
    var meta MetaFile
    if err := json.Unmarshal(data, &meta); err != nil {
        return MetaFile{}, fmt.Errorf("failed to parse meta JSON %s: %w", path, err)
    }
    return meta, nil
}

// LoadDBC reads the DBC file and parses it into memory
func LoadDBC(dbcPath string, meta MetaFile) (DBCFile, error) {
    data, err := os.ReadFile(dbcPath)
    if err != nil {
        return DBCFile{}, fmt.Errorf("failed to read DBC file %s: %w", dbcPath, err)
    }
    if len(data) < 20 {
        return DBCFile{}, fmt.Errorf("file too small to be a valid DBC: %s", dbcPath)
    }

    header, err := ParseHeader(data[:20])
    if err != nil {
        return DBCFile{}, err
    }

    recordsStart := 20
    stringBlockStart := recordsStart + int(header.RecordCount*header.RecordSize)
    if stringBlockStart+int(header.StringBlockSize) > len(data) {
        return DBCFile{}, fmt.Errorf("file too small for records + string block: %s", dbcPath)
    }
    stringBlock := data[stringBlockStart : stringBlockStart+int(header.StringBlockSize)]

    records, err := ParseRecords(data, recordsStart, header, meta, stringBlock)
    if err != nil {
        return DBCFile{}, err
    }

    return DBCFile{
        Header:      header,
        Records:     records,
        StringBlock: stringBlock,
    }, nil
}

// ParseHeader parses the DBC header
func ParseHeader(data []byte) (DBCHeader, error) {
    header := DBCHeader{
        Magic:           [4]byte{data[0], data[1], data[2], data[3]},
        RecordCount:     binary.LittleEndian.Uint32(data[4:8]),
        FieldCount:      binary.LittleEndian.Uint32(data[8:12]),
        RecordSize:      binary.LittleEndian.Uint32(data[12:16]),
        StringBlockSize: binary.LittleEndian.Uint32(data[16:20]),
    }
    if string(header.Magic[:]) != "WDBC" {
        return DBCHeader{}, fmt.Errorf("invalid DBC file magic: %s", string(header.Magic[:]))
    }
    return header, nil
}

// ParseRecords reads all records into memory
func ParseRecords(data []byte, start int, header DBCHeader, meta MetaFile, stringBlock []byte) ([]Record, error) {
    var records []Record
    for i := uint32(0); i < header.RecordCount; i++ {
        rec := make(Record)
        recordOffset := start + int(i*header.RecordSize)
        offset := 0

        for _, field := range meta.Fields {
            repeat := int(field.Count)
            if repeat == 0 {
                repeat = 1
            }

            for j := 0; j < repeat; j++ {
                name := field.Name
                if field.Count > 1 {
                    name = fmt.Sprintf("%s_%d", field.Name, j+1)
                }

                switch field.Type {
                case "int32":
                    val := int32(binary.LittleEndian.Uint32(data[recordOffset+offset : recordOffset+offset+4]))
                    rec[name] = val
                    offset += 4

                case "uint32":
                    val := binary.LittleEndian.Uint32(data[recordOffset+offset : recordOffset+offset+4])
                    rec[name] = val
                    offset += 4

                case "float":
                    bits := binary.LittleEndian.Uint32(data[recordOffset+offset : recordOffset+offset+4])
                    rec[name] = math.Float32frombits(bits)
                    offset += 4

                case "string":
                    strOffset := binary.LittleEndian.Uint32(data[recordOffset+offset : recordOffset+offset+4])
                    rec[name] = strOffset
                    offset += 4

                case "Loc":
                    loc := make([]uint32, 17)
                    for col := 0; col < 17; col++ {
                        val := binary.LittleEndian.Uint32(data[recordOffset+offset : recordOffset+offset+4])
                        loc[col] = val
                        offset += 4
                    }
                    rec[name] = loc

                default:
                    return nil, fmt.Errorf("unknown field type: %s", field.Type)
                }
            }
        }

        records = append(records, rec)
    }

    return records, nil
}

func ReadDBCHeader(dbcName string, cfg *Config) (DBCHeader, error) {
	dbcPath := filepath.Join(cfg.Paths.Base, dbcName+".dbc")

	// Check existence
	if _, err := os.Stat(dbcPath); os.IsNotExist(err) {
		return DBCHeader{}, fmt.Errorf("DBC file not found: %s", dbcPath)
	}

	data, err := os.ReadFile(dbcPath)
	if err != nil {
		return DBCHeader{}, fmt.Errorf("failed to read DBC file: %w", err)
	}
	if len(data) < 20 {
		return DBCHeader{}, fmt.Errorf("file too small to contain a valid DBC header")
	}

	header, err := ParseHeader(data[:20])
    if err != nil {
        return DBCHeader{}, err
    }
    
	return header, nil
}

func ReadDBCFile(dbcName string, cfg *Config) (*DBCFile, *MetaFile, error) {
	dbcPath := filepath.Join(cfg.Paths.Base, dbcName+".dbc")
	metaPath := filepath.Join(cfg.Paths.Meta, dbcName+".meta.json")

	if _, err := os.Stat(dbcPath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("DBC file not found: %s", dbcPath)
	}
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("Meta file not found: %s", metaPath)
	}

	meta, err := LoadMeta(metaPath)
    if err != nil {
        return nil, nil, fmt.Errorf("failed to load meta: %w", err)
    }

    dbc, err := LoadDBC(dbcPath, meta)
    if err != nil {
        return nil, nil, fmt.Errorf("failed to load dbc: %w", err)
    }

	return &dbc, &meta, nil
}

// WriteDBC writes a DBC file from memory
func WriteDBC(dbc *DBCFile, meta *MetaFile, outPath string) error {
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Write header
	headerBuf := make([]byte, 20)
	copy(headerBuf[0:4], dbc.Header.Magic[:])
	binary.LittleEndian.PutUint32(headerBuf[4:8], dbc.Header.RecordCount)
	binary.LittleEndian.PutUint32(headerBuf[8:12], dbc.Header.FieldCount)
	binary.LittleEndian.PutUint32(headerBuf[12:16], dbc.Header.RecordSize)
	binary.LittleEndian.PutUint32(headerBuf[16:20], dbc.Header.StringBlockSize)
	if _, err := outFile.Write(headerBuf); err != nil {
		return err
	}

	// Write records
	recordData := make([]byte, dbc.Header.RecordCount*dbc.Header.RecordSize)
	offset := 0

	for _, rec := range dbc.Records {
		for _, field := range meta.Fields {
			repeat := int(field.Count)
			if repeat == 0 {
				repeat = 1
			}

			for j := 0; j < repeat; j++ {
				name := field.Name
				if field.Count > 1 {
					name = fmt.Sprintf("%s_%d", field.Name, j+1)
				}

				switch field.Type {
				case "int32":
					binary.LittleEndian.PutUint32(recordData[offset:offset+4],uint32(rec[name].(int32)))
					offset += 4
				case "uint32":
					binary.LittleEndian.PutUint32(recordData[offset:offset+4],rec[name].(uint32))
					offset += 4
				case "float":
					bits := math.Float32bits(rec[name].(float32))
					binary.LittleEndian.PutUint32(recordData[offset:offset+4],bits)
					offset += 4
				case "string":
					binary.LittleEndian.PutUint32(recordData[offset:offset+4],rec[name].(uint32))
					offset += 4

				case "Loc":
					loc := rec[name].([]uint32)
					for _, v := range loc {
						binary.LittleEndian.PutUint32(recordData[offset:offset+4], v)
						offset += 4
					}
				}
			}
		}
	}

	if _, err := outFile.Write(recordData); err != nil {
		return err
	}

	// Write string block
	if _, err := outFile.Write(dbc.StringBlock); err != nil {
		return err
	}

	return nil
}

// --- Utility Functions ---
func readString(stringBlock []byte, offset uint32) string {
	if offset >= uint32(len(stringBlock)) {
		return ""
	}
	end := offset
	for end < uint32(len(stringBlock)) && stringBlock[end] != 0 {
		end++
	}
	return string(stringBlock[offset:end])
}

func PrintRecord(rec Record, meta *MetaFile, stringBlock []byte) {
    for _, field := range meta.Fields {
        repeat := int(field.Count)
        if repeat == 0 {
            repeat = 1
        }

        for j := 0; j < repeat; j++ {
            name := field.Name
            if field.Count > 1 {
                name = fmt.Sprintf("%s_%d", field.Name, j+1)
            }

            val, exists := rec[name]
            if !exists {
                fmt.Printf("  %s: <missing>\n", name)
                continue
            }

            switch field.Type {
            case "string":
                offset := val.(uint32)
                str := readString(stringBlock, offset)
                fmt.Printf("  %s: %v (\"%s\")\n", name, offset, str)
            case "Loc":
                locArr := val.([]uint32)
                for i, lang := range locLangs {
                    if i < len(locArr)-1 {
                        str := readString(stringBlock, locArr[i])
                        fmt.Printf("  %s_%s: %v (\"%s\")\n", name, lang, locArr[i], str)
                    } else {
                        fmt.Printf("  %s_flags: %v\n", name, locArr[i])
                    }
                }
            default:
                fmt.Printf("  %s: %v\n", name, val)
            }
        }
    }
}