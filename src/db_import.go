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

// ImportDBCs scans the meta directory and imports all DBCs
func ImportDBCs(db *sql.DB, cfg *Config) error {
	metas, err := filepath.Glob(filepath.Join(cfg.Paths.Meta, "*.meta.json"))
	if err != nil {
		return fmt.Errorf("failed to scan meta directory: %w", err)
	}

	for _, metaPath := range metas {
		if err := ImportDBC(db, cfg, metaPath); err != nil {
			return err
		}
	}

	return nil
}

// ImportDBC imports a single DBC into SQL based on its meta
func ImportDBC(db *sql.DB, cfg *Config, metaPath string) error {
	meta, err := LoadMeta(metaPath)
	if err != nil {
		return fmt.Errorf("failed to load meta %s: %w", metaPath, err)
	}

	tableName := strings.TrimSuffix(filepath.Base(meta.File), ".dbc")
	dbcPath := filepath.Join(cfg.Paths.Base, meta.File)

	if _, err := os.Stat(dbcPath); os.IsNotExist(err) {
		log.Printf("Skipping %s: DBC file does not exist", tableName)
		return nil
	}

	if tableExists(db, tableName) {
		log.Printf("Skipping %s: table already exists", tableName)
		return nil
	}

	log.Printf("Importing %s into table %s...", dbcPath, tableName)

	dbc, err := LoadDBC(dbcPath, meta)
	if err != nil {
		return fmt.Errorf("failed to load DBC %s: %w", dbcPath, err)
	}

	checkUniqueKeys(dbc.Records, &meta, tableName)

	if err := createTable(db, tableName, &meta); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	if err := insertRecords(db, tableName, &dbc, &meta); err != nil {
		return fmt.Errorf("failed to insert records for %s: %w", tableName, err)
	}

	log.Printf("Imported %s into table %s", dbcPath, tableName)
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
        repeat := int(field.Count)
        if repeat == 0 {
            repeat = 1
        }

        for j := 0; j < repeat; j++ {
            colName := field.Name
            if field.Count > 1 {
                colName = fmt.Sprintf("%s_%d", field.Name, j+1)
            }

            switch field.Type {
            case "int32":
                columns = append(columns, fmt.Sprintf("`%s` INT", colName))
            case "uint32":
                columns = append(columns, fmt.Sprintf("`%s` BIGINT UNSIGNED", colName))
            case "float":
                columns = append(columns, fmt.Sprintf("`%s` FLOAT", colName))
            case "string":
                columns = append(columns, fmt.Sprintf("`%s` TEXT", colName))
            case "Loc":
                for i, lang := range locLangs {
                    locCol := fmt.Sprintf("%s_%s", colName, lang)
                    if i == len(locLangs)-1 {
                        // last element → flags as INT UNSIGNED
                        columns = append(columns, fmt.Sprintf("`%s` INT UNSIGNED", locCol))
                    } else {
                        columns = append(columns, fmt.Sprintf("`%s` TEXT", locCol))
                    }
                }
            default:
                return fmt.Errorf("unknown field type: %s", field.Type)
            }
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
	total := len(dbc.Records)
	if total == 0 {
		return nil
	}

	// Transaction is optional, but speeds things up if you’re inserting many rows
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // safe rollback if Commit not reached

	columnsBase := make([]string, 0, len(meta.Fields)*len(locLangs))
    for _, field := range meta.Fields {
        repeat := int(field.Count)
        if repeat == 0 {
            repeat = 1
        }

        for j := 0; j < repeat; j++ {
            colName := field.Name
            if field.Count > 1 {
                colName = fmt.Sprintf("%s_%d", field.Name, j+1)
            }
            switch field.Type {
            case "int32", "uint32", "float", "string":
                columnsBase = append(columnsBase, fmt.Sprintf("`%s`", colName))
            case "Loc":
                for _, lang := range locLangs {
                    columnsBase = append(columnsBase, fmt.Sprintf("`%s_%s`", colName, lang))
                }
            }
        }
    }
    
    // calculate batch size
    colsPerRow := len(columnsBase)
    maxPlaceholders := 60000 // stay below 65535 max batch size
    batchSize := maxPlaceholders / colsPerRow

    if batchSize > 2000 {
        batchSize = 2000
    }

	// process in batches
	for start := 0; start < total; start += batchSize {
		end := start + batchSize
		if end > total {
			end = total
		}
		records := dbc.Records[start:end]

		var allPlaceholders []string
		var allValues []interface{}

        for _, rec := range records {
            var rowPlaceholders []string

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
                    case "int32", "uint32", "float":
                        rowPlaceholders = append(rowPlaceholders, "?")
                        allValues = append(allValues, rec[name])
                    case "string":
                        rowPlaceholders = append(rowPlaceholders, "?")
                        offset := rec[name].(uint32)
                        allValues = append(allValues, readString(dbc.StringBlock, offset))
                    case "Loc":
                        locArr := rec[name].([]uint32)
                        numTexts := len(locArr) - 1
                        for i := range locLangs {
                            if i < numTexts {
                                allValues = append(allValues, readString(dbc.StringBlock, locArr[i]))
                            } else if i == numTexts {
                                allValues = append(allValues, locArr[numTexts]) // flags
                            } else {
                                allValues = append(allValues, nil) // extra unused
                            }
                            rowPlaceholders = append(rowPlaceholders, "?")
                        }
                    }
                }
            }

            allPlaceholders = append(allPlaceholders, "("+strings.Join(rowPlaceholders, ", ")+")")
        }

		query := fmt.Sprintf(
			"INSERT INTO `%s` (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			tableName,
			strings.Join(columnsBase, ", "),
			strings.Join(allPlaceholders, ", "),
			generateUpdateAssignments(columnsBase),
		)

		if _, err := tx.Exec(query, allValues...); err != nil {
			return fmt.Errorf("batch insert failed (%d–%d): %v", start, end, err)
		}

		fmt.Printf("Inserted batch %d–%d of %d\n", start+1, end, total)
	}

	if err := tx.Commit(); err != nil {
		return err
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
