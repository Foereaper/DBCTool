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
	"enus", "kokr", "frfr", "dede", "zhcn", "zhtw",
	"eses", "esmx", "ruru", "jajp", "ptpt", "itit",
	"unused_1", "unused_2", "unused_3", "unused_4", "flags",
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
    if err := ensureChecksumTable(db); err != nil {
        return fmt.Errorf("failed to ensure dbc_checksum table: %w", err)
    }
    
	meta, err := LoadMeta(metaPath)
	if err != nil {
		return fmt.Errorf("failed to load meta %s: %w", metaPath, err)
	}

	tableName := strings.TrimSuffix(filepath.Base(meta.File), ".dbc")
    if meta.TableName != "" {
        tableName = meta.TableName
    }
    
	dbcPath := filepath.Join(cfg.Paths.Base, meta.File)

	if _, err := os.Stat(dbcPath); os.IsNotExist(err) {
		log.Printf("Skipping %s: DBC file does not exist", tableName)
		return nil
	}
    
    if err := ensureChecksumEntry(db, tableName); err != nil {
        return fmt.Errorf("failed to ensure checksum entry for %s: %w", tableName, err)
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

	validFields := make(map[string]struct{})
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
				columns = append(columns, fmt.Sprintf("`%s` INT UNSIGNED", colName))
			case "float":
				columns = append(columns, fmt.Sprintf("`%s` DECIMAL(38,16)", colName))
			case "string":
				columns = append(columns, fmt.Sprintf("`%s` TEXT", colName))
			case "Loc":
				for i, lang := range locLangs {
					locCol := fmt.Sprintf("%s_%s", colName, lang)
					if i == len(locLangs)-1 {
						columns = append(columns, fmt.Sprintf("`%s` INT UNSIGNED", locCol))
					} else {
						columns = append(columns, fmt.Sprintf("`%s` TEXT", locCol))
					}
				}
			default:
				return fmt.Errorf("unknown field type: %s", field.Type)
			}

			// track valid field name
			validFields[colName] = struct{}{}
		}
	}

	// Default PK handling
	pkCols := []string{"`auto_id`"} // default if nothing set
	if len(meta.PrimaryKeys) > 0 {
		var validPKs []string
		for _, pkc := range meta.PrimaryKeys {
			if _, ok := validFields[pkc]; ok {
				validPKs = append(validPKs, fmt.Sprintf("`%s`", pkc))
			}
		}

		if len(validPKs) > 0 {
			pkCols = validPKs
		} else {
			// fallback: add surrogate key
            log.Printf("No valid primary keys found for %s; using auto-increment surrogate key `auto_id`", tableName)
			columns = append([]string{"`auto_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"}, columns...)
			pkCols = []string{"`auto_id`"}
		}
	}

	// Build CREATE TABLE
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (%s, PRIMARY KEY(%s)",
		tableName, strings.Join(columns, ", "), strings.Join(pkCols, ", "),
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
    // stay below 65535 max batch size
    maxPlaceholders := 60000
    batchSize := maxPlaceholders / colsPerRow
    if batchSize > 2000 {
        batchSize = 2000
    }

    // progress tracking
    nextPercent := 15

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

        // progress check
        done := end * 100 / total
        if done >= nextPercent {
            log.Printf("%d%% complete.. (%d/%d rows)\n", done, end, total)
            nextPercent += 15
        }
    }

    if err := tx.Commit(); err != nil {
        return err
    }

    log.Println("100% complete!")

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

// ensureChecksumTable ensures the dbc_checksum table exists
func ensureChecksumTable(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS dbc_checksum (
		table_name VARCHAR(255) NOT NULL PRIMARY KEY,
		checksum BIGINT UNSIGNED NOT NULL DEFAULT 0
	)`
	_, err := db.Exec(query)
	return err
}

// ensureChecksumEntry makes sure a row for the table exists in dbc_checksum
func ensureChecksumEntry(db *sql.DB, tableName string) error {
	var exists int
	err := db.QueryRow("SELECT 1 FROM dbc_checksum WHERE table_name = ?", tableName).Scan(&exists)
	if err == sql.ErrNoRows {
		_, insErr := db.Exec("INSERT INTO dbc_checksum (table_name, checksum) VALUES (?, 0)", tableName)
		return insErr
	}
	return err
}