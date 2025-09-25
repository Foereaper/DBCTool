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
	"strings"
    "sort"
)

var locLangs = []string{
	"enUS", "koKR", "frFR", "deDE", "zhCN", "zhTW",
	"esES", "esMX", "ruRU", "jaJP", "ptPT", "itIT",
	"unused1", "unused2", "unused3", "unused4", "flags",
}

// ImportDBCs scans the meta directory and imports DBCs into SQL, with duplicate check
func ImportDBCs(db *sql.DB, cfg *Config) error {
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

		dbcPath := filepath.Join(cfg.Paths.Base, meta.File)
		if _, err := os.Stat(dbcPath); os.IsNotExist(err) {
			log.Printf("Skipping %s: DBC file does not exist", tableName)
			continue
		}

		// Check if table exists
		if tableExists(db, tableName) {
			log.Printf("Skipping %s: table already exists", tableName)
			continue
		}

		log.Printf("Importing %s into table %s...", dbcPath, tableName)

		// Load DBC
		dbc, err := LoadDBC(dbcPath, meta)
        if err != nil {
            return fmt.Errorf("failed to load dbc: %w", err)
        }

		// --- NEW: Check duplicates based on uniqueKeys ---
		checkUniqueKeys(dbc.Records, &meta, tableName)

		// Create table
		if err := createTable(db, tableName, &meta); err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Insert records
		if err := insertRecords(db, tableName, &dbc, &meta); err != nil {
			return fmt.Errorf("failed to insert records for %s: %w", tableName, err)
		}

		log.Printf("Imported %s into table %s", dbcPath, tableName)
	}

	return nil
}

// checkUniqueKeys scans records for duplicates based on meta.UniqueKeys
func checkUniqueKeys(records []Record, meta *MetaFile, tableName string) {
	for i, uk := range meta.UniqueKeys {
		if len(uk) == 0 {
			continue
		}

		seen := map[string][]int{} // map[keyString] -> list of record indices

		for idx, rec := range records {
			var keyParts []string
			for _, col := range uk {
				val, ok := rec[col]
				if !ok {
					val = "<MISSING>"
				}
				keyParts = append(keyParts, fmt.Sprintf("%v", val))
			}

			keyStr := strings.Join(keyParts, ":")
			seen[keyStr] = append(seen[keyStr], idx)
		}

		for _, indices := range seen {
			if len(indices) > 1 {
				fmt.Printf("\nWarning: duplicate records found in table '%s' for unique key #%d (%v):\n",
					tableName, i, uk)
				for _, idx := range indices {
					fmt.Printf("  Record %d: {\n", idx)
					rec := records[idx]
					keys := make([]string, 0, len(rec))
					for k := range rec {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					for _, k := range keys {
						fmt.Printf("    %s: %v\n", k, rec[k])
					}
					fmt.Println("  }")
				}
			}
		}
	}
}

// tableExists checks if a table already exists
func tableExists(db *sql.DB, table string) bool {
	var exists string
	err := db.QueryRow("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		log.Printf("Warning: could not check table %s: %v", table, err)
		return false
	}
	return true
}

// createTable constructs table based on meta, Loc fields, and unique keys
func createTable(db *sql.DB, tableName string, meta *MetaFile) error {
	var columns []string

	for _, field := range meta.Fields {
		switch field.Type {
		case "int32":
			columns = append(columns, fmt.Sprintf("`%s` INT", field.Name))
		case "uint32":
			columns = append(columns, fmt.Sprintf("`%s` BIGINT UNSIGNED", field.Name))
		case "float":
			columns = append(columns, fmt.Sprintf("`%s` FLOAT", field.Name))
		case "string":
			columns = append(columns, fmt.Sprintf("`%s` TEXT", field.Name))
		case "Loc":
			for i, lang := range locLangs {
				colName := fmt.Sprintf("%s_%s", field.Name, lang)
				if i == len(locLangs)-1 {
					// last element → flags as INT UNSIGNED
					columns = append(columns, fmt.Sprintf("`%s` INT UNSIGNED", colName))
				} else {
					columns = append(columns, fmt.Sprintf("`%s` TEXT", colName))
				}
			}
		default:
			return fmt.Errorf("unknown field type: %s", field.Type)
		}
	}

	// Default primary key
    pk := "`ID`"
    if len(meta.PrimaryKeys) > 0 {
        pkCols := make([]string, len(meta.PrimaryKeys))
        for i, pkc := range meta.PrimaryKeys {
            pkCols[i] = fmt.Sprintf("`%s`", pkc)
        }
        pk = strings.Join(pkCols, ", ")
    }

	// Start building CREATE TABLE query
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (%s, PRIMARY KEY(%s)",
		tableName, strings.Join(columns, ", "), pk,
	)

	// Add unique keys dynamically
	for i, uk := range meta.UniqueKeys {
		if len(uk) == 0 {
			continue
		}
		cols := make([]string, len(uk))
		for j, c := range uk {
			cols[j] = fmt.Sprintf("`%s`", c)
		}
		query += fmt.Sprintf(", UNIQUE KEY `uk_%d` (%s)", i, strings.Join(cols, ", "))
	}

	query += ")"

	_, err := db.Exec(query)
	return err
}

// insertRecords inserts all DBC records into SQL
func insertRecords(db *sql.DB, tableName string, dbc *DBCFile, meta *MetaFile) error {
	for _, rec := range dbc.Records {
		var columns []string
		var placeholders []string
		var values []interface{}

		for _, field := range meta.Fields {
			switch field.Type {
			case "int32", "uint32", "float":
				columns = append(columns, fmt.Sprintf("`%s`", field.Name))
				placeholders = append(placeholders, "?")
				values = append(values, rec[field.Name])
			case "string":
				columns = append(columns, fmt.Sprintf("`%s`", field.Name))
				placeholders = append(placeholders, "?")
				offset := rec[field.Name].(uint32)
				values = append(values, readString(dbc.StringBlock, offset))
			case "Loc":
				locArr := rec[field.Name].([]uint32)
				numTexts := len(locArr) - 1 // last element is flags
				for i, lang := range locLangs {
					colName := fmt.Sprintf("%s_%s", field.Name, lang)
					columns = append(columns, fmt.Sprintf("`%s`", colName))
					placeholders = append(placeholders, "?")

					if i < numTexts {
						// text field
						values = append(values, readString(dbc.StringBlock, locArr[i]))
					} else if i == numTexts {
						// flags
						values = append(values, locArr[numTexts])
					} else {
						// extra unused
						values = append(values, nil)
					}
				}
			}
		}

		query := fmt.Sprintf(
			"INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
			tableName,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
			generateUpdateAssignments(columns),
		)

		if _, err := db.Exec(query, values...); err != nil {
			return fmt.Errorf("insert failed: %v", err)
		}
	}

	return nil
}

// generateUpdateAssignments generates the ON DUPLICATE KEY UPDATE clause
func generateUpdateAssignments(columns []string) string {
	assignments := make([]string, len(columns))
	for i, col := range columns {
		assignments[i] = fmt.Sprintf("%s=VALUES(%s)", col, col)
	}
	return strings.Join(assignments, ", ")
}
