// Copyright (c) 2025 DBCTool
//
// DBCTool is licensed under the MIT License.
// See the LICENSE file for details.

package main

import (
	"database/sql"
	"fmt"
    "log"
	"os"
	"path/filepath"
    "strconv"
	"strings"
)

// ExportDBCs iterates over all meta files and exports each table
func ExportDBCs(db *sql.DB, cfg *Config) error {
    metas, err := filepath.Glob(filepath.Join(cfg.Paths.Meta, "*.meta.json"))
    if err != nil {
        return fmt.Errorf("failed to scan meta directory: %w", err)
    }

    for _, metaPath := range metas {
        if err := ExportDBC(db, cfg, metaPath); err != nil {
            return fmt.Errorf("failed to export %s: %w", metaPath, err)
        }
    }

    return nil
}

// ExportDBC handles exporting a single table/meta to a DBC file
func ExportDBC(db *sql.DB, cfg *Config, metaPath string) error {
    meta, err := LoadMeta(metaPath)
	if err != nil {
		return fmt.Errorf("failed to load meta %s: %w", metaPath, err)
	}
    
    tableName := strings.TrimSuffix(meta.File, ".dbc")
    
    // Ensure checksum table & entry exist
    if err := ensureChecksumTable(db); err != nil {
        return fmt.Errorf("failed to ensure dbc_checksum table: %w", err)
    }
    
    if err := ensureChecksumEntry(db, tableName); err != nil {
        return fmt.Errorf("failed to ensure checksum entry for %s: %w", tableName, err)
    }

    // Compare checksums
    currentCS, err := getTableChecksum(db, tableName)
    if err != nil {
        return fmt.Errorf("failed to calculate checksum for %s: %w", tableName, err)
    }

    storedCS, err := getStoredChecksum(db, tableName)
    if err != nil {
        return fmt.Errorf("failed to get stored checksum for %s: %w", tableName, err)
    }

    if (currentCS == storedCS) && cfg.Options.UseVersioning {
        log.Printf("Skipping %s: no changes detected", tableName)
        return nil
    }
    
    log.Printf("Exporting table %s to DBC...\n", tableName)
    
    orderClause := buildOrderBy(meta.SortOrder)
    
    rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`%s", tableName, orderClause))
    if err != nil {
        return fmt.Errorf("failed to query table %s: %w", tableName, err)
    }
    defer rows.Close()

    cols, err := rows.Columns()
    if err != nil {
        return fmt.Errorf("failed to get columns for table %s: %w", tableName, err)
    }

    dbc := DBCFile{
        Header:      DBCHeader{Magic: [4]byte{'W', 'D', 'B', 'C'}},
        Records:     []Record{},
        StringBlock: []byte{0}, // first byte must be null
    }
    stringOffsets := map[string]uint32{"": 0}

    for rows.Next() {
        raw := make([]interface{}, len(cols))
        ptrs := make([]interface{}, len(cols))
        for i := range raw {
            ptrs[i] = &raw[i]
        }
        if err := rows.Scan(ptrs...); err != nil {
            return fmt.Errorf("failed to scan row for table %s: %w", tableName, err)
        }

        rec := make(Record)
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
                    rec[name] = toInt32(raw, cols, name)
                case "uint32":
                    rec[name] = toUint32(raw, cols, name)
                case "float":
                    rec[name] = toFloat32(raw, cols, name)
                case "string":
                    str := toString(raw, cols, name)
                    rec[name] = getStringOffset(str, &dbc.StringBlock, stringOffsets)
                case "Loc":
                    loc := make([]uint32, 17)
                    for i := 0; i < 16; i++ {
                        colName := fmt.Sprintf("%s_%s", name, locLangs[i])
                        str := toString(raw, cols, colName)
                        loc[i] = getStringOffset(str, &dbc.StringBlock, stringOffsets)
                    }
                    loc[16] = toUint32(raw, cols, fmt.Sprintf("%s_flags", name))
                    rec[name] = loc
                }
            }
        }
        dbc.Records = append(dbc.Records, rec)
    }

    dbc.Header.RecordCount = uint32(len(dbc.Records))
    dbc.Header.FieldCount = calculateFieldCount(meta)
    dbc.Header.RecordSize = calculateRecordSize(meta)
    dbc.Header.StringBlockSize = uint32(len(dbc.StringBlock))

    outPath := filepath.Join(cfg.Paths.Export, meta.File)
    if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
        return fmt.Errorf("failed to create export directory: %w", err)
    }

    if err := WriteDBC(&dbc, &meta, outPath); err != nil {
        return fmt.Errorf("failed to write DBC %s: %w", outPath, err)
    }
    
    if err := updateChecksum(db, tableName, currentCS); err != nil {
        return fmt.Errorf("failed to update checksum for %s: %w", tableName, err)
    }

    log.Printf("Exported %s\n", meta.File)
    return nil
}

// --- Helpers ---

func buildOrderBy(sort []SortField) string {
	if len(sort) == 0 {
		return ""
	}
	parts := make([]string, len(sort))
	for i, sf := range sort {
		dir := strings.ToUpper(sf.Direction)
		if dir != "ASC" && dir != "DESC" {
			dir = "ASC"
		}
		parts[i] = fmt.Sprintf("`%s` %s", sf.Name, dir)
	}
	return " ORDER BY " + strings.Join(parts, ", ")
}

func getStringOffset(s string, block *[]byte, offsets map[string]uint32) uint32 {
	if off, ok := offsets[s]; ok {
		return off
	}
	off := uint32(len(*block))
	*block = append(*block, []byte(s)...)
	*block = append(*block, 0)
	offsets[s] = off
	return off
}

func calculateRecordSize(meta MetaFile) uint32 {
	size := 0
	for _, f := range meta.Fields {
		repeat := int(f.Count)
		if repeat == 0 {
			repeat = 1
		}

		for j := 0; j < repeat; j++ {
			switch f.Type {
			case "int32", "uint32", "float", "string":
				size += 4
			case "Loc":
				size += 4 * 17
			}
		}
	}
	return uint32(size)
}

func calculateFieldCount(meta MetaFile) uint32 {
	count := 0
	for _, f := range meta.Fields {
		repeat := int(f.Count)
		if repeat == 0 {
			repeat = 1
		}

		for j := 0; j < repeat; j++ {
			if f.Type == "Loc" {
				count += 17
			} else {
				count++
			}
		}
	}
	return uint32(count)
}

func toInt32(raw []interface{}, cols []string, name string) int32 {
	for i, col := range cols {
		if col == name && raw[i] != nil {
			if v, ok := raw[i].(int64); ok {
				return int32(v)
			}
		}
	}
	return 0
}

func toUint32(raw []interface{}, cols []string, name string) uint32 {
	for i, col := range cols {
		if col == name && raw[i] != nil {
			switch v := raw[i].(type) {
			case int64:
				return uint32(v)
			case uint64:
				return uint32(v)
			}
		}
	}
	return 0
}

func toFloat32(raw []interface{}, cols []string, name string) float32 {
	for i, col := range cols {
		if col == name && raw[i] != nil {
			switch v := raw[i].(type) {
			case float64:
				return float32(v)
			case float32:
				return v
			case []byte:
				if f, err := strconv.ParseFloat(string(v), 64); err == nil {
					return float32(f)
				}
			case string:
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					return float32(f)
				}
			}
		}
	}
	return 0
}

func toString(raw []interface{}, cols []string, name string) string {
	for i, col := range cols {
		if col == name && raw[i] != nil {
			switch v := raw[i].(type) {
			case string:
				return v
			case []byte:
				return string(v)
			}
		}
	}
	return ""
}

// getTableChecksum returns the CHECKSUM TABLE value
func getTableChecksum(db *sql.DB, tableName string) (uint64, error) {
	var tbl string
	var checksum sql.NullInt64
	err := db.QueryRow("CHECKSUM TABLE `" + tableName + "`").Scan(&tbl, &checksum)
	if err != nil {
		return 0, err
	}
	if !checksum.Valid {
		return 0, nil
	}
	return uint64(checksum.Int64), nil
}

// getStoredChecksum retrieves the stored checksum from dbc_checksum
func getStoredChecksum(db *sql.DB, tableName string) (uint64, error) {
	var cs sql.NullInt64
	err := db.QueryRow("SELECT checksum FROM dbc_checksum WHERE table_name = ?", tableName).Scan(&cs)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !cs.Valid {
		return 0, nil
	}
	return uint64(cs.Int64), nil
}

// updateChecksum updates the stored checksum for a table
func updateChecksum(db *sql.DB, tableName string, checksum uint64) error {
	_, err := db.Exec("UPDATE dbc_checksum SET checksum = ? WHERE table_name = ?", checksum, tableName)
	return err
}