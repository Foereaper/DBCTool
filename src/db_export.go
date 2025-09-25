// Copyright (c) 2025 DBCTool
//
// DBCTool is licensed under the MIT License.
// See the LICENSE file for details.

package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ExportDBCs queries SQL tables and rebuilds DBC files using DBCFile + MetaFile
func ExportDBCs(db *sql.DB, cfg *Config) error {
	metas, err := filepath.Glob(filepath.Join(cfg.Paths.Meta, "*.meta.json"))
	if err != nil {
		return fmt.Errorf("failed to scan meta directory: %w", err)
	}

	for _, metaPath := range metas {
		meta, err := LoadMeta(metaPath)
        if err != nil {
            return fmt.Errorf("failed to load meta: %w", err)
        }
		tableName := strings.TrimSuffix(filepath.Base(meta.File), ".dbc")

		// Skip missing tables
		if !tableExists(db, tableName) {
			fmt.Printf("Skipping %s: table does not exist\n", tableName)
			continue
		}

		fmt.Printf("Exporting table %s to DBC...\n", tableName)

		orderClause := buildOrderBy(meta.SortOrder)
        rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`%s", tableName, orderClause))
        if err != nil {
            return fmt.Errorf("failed to query table %s: %w", tableName, err)
        }
        
		cols, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get columns: %w", err)
		}

		// Initialize DBCFile
		dbc := DBCFile{
			Header:      DBCHeader{Magic: [4]byte{'W', 'D', 'B', 'C'}},
			Records:     make([]Record, 0),
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
				return fmt.Errorf("row scan failed: %w", err)
			}

			rec := make(Record)
			// build strictly in meta.Fields order
			for _, field := range meta.Fields {
				switch field.Type {
				case "int32":
					rec[field.Name] = toInt32(raw, cols, field.Name)
				case "uint32":
					rec[field.Name] = toUint32(raw, cols, field.Name)
				case "float":
					rec[field.Name] = toFloat32(raw, cols, field.Name)
				case "string":
					str := toString(raw, cols, field.Name)
					rec[field.Name] = getStringOffset(str, &dbc.StringBlock, stringOffsets)
				case "Loc":
					loc := make([]uint32, 17)
					for i := 0; i < 16; i++ {
						colName := fmt.Sprintf("%s_%s", field.Name, locLangs[i])
						str := toString(raw, cols, colName)
						loc[i] = getStringOffset(str, &dbc.StringBlock, stringOffsets)
					}
					loc[16] = toUint32(raw, cols, fmt.Sprintf("%s_flags", field.Name))
					rec[field.Name] = loc
				}
			}

			dbc.Records = append(dbc.Records, rec)
		}

		// Recalculate header
		dbc.Header.RecordCount = uint32(len(dbc.Records))
		dbc.Header.FieldCount = calculateFieldCount(meta)
		dbc.Header.RecordSize = calculateRecordSize(meta)
		dbc.Header.StringBlockSize = uint32(len(dbc.StringBlock))

		// Sanity check
		if dbc.Header.RecordSize%4 != 0 {
			return fmt.Errorf("record size %d not multiple of 4", dbc.Header.RecordSize)
		}

		// Write out
		outPath := filepath.Join(cfg.Paths.Export, meta.File)
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return fmt.Errorf("failed to create export dir: %w", err)
		}
		if err := WriteDBC(&dbc, &meta, outPath); err != nil {
			return fmt.Errorf("failed to rebuild %s: %w", meta.File, err)
		}

		fmt.Printf("Exported %s (%d records)\n", outPath, dbc.Header.RecordCount)
	}

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
		switch f.Type {
		case "int32", "uint32", "float", "string":
			size += 4
		case "Loc":
			size += 4 * 17
		}
	}
	return uint32(size)
}

func calculateFieldCount(meta MetaFile) uint32 {
	count := 0
	for _, f := range meta.Fields {
		if f.Type == "Loc" {
			count += 17
		} else {
			count++
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
			if v, ok := raw[i].(float64); ok {
				return float32(v)
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
