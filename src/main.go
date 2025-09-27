// Copyright (c) 2025 DBCTool
//
// DBCTool is licensed under the MIT License.
// See the LICENSE file for details.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
    "strings"
)

func main() {
	// If no args at all -> usage
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	// Extract global config flag from os.Args (support: --config=path, --config path, -config, -config=)
	// Default:
	globalConfigPath := "./config.json"

	// We'll build a new slice of args to pass to subcommand flags (argsWithoutGlobal).
	argsWithoutGlobal := []string{os.Args[0]}
	i := 1
	for i < len(os.Args) {
		a := os.Args[i]
		if a == "--config" || a == "-config" || a == "-c" {
			// next arg should be value
			if i+1 < len(os.Args) {
				globalConfigPath = os.Args[i+1]
				i += 2
				continue
			} else {
				fmt.Println("Error: --config requires a value")
				return
			}
		}
		// handle forms like --config=/path or -config=/path
		if strings.HasPrefix(a, "--config=") || strings.HasPrefix(a, "-config=") {
			parts := strings.SplitN(a, "=", 2)
			if len(parts) == 2 {
				globalConfigPath = parts[1]
				i++
				continue
			}
		}
		// not a global flag, keep for subcommands
		argsWithoutGlobal = append(argsWithoutGlobal, a)
		i++
	}

	// Now argsWithoutGlobal[1] should be the command
	if len(argsWithoutGlobal) < 2 {
		printUsage()
		return
	}
	cmd := argsWithoutGlobal[1]
	// Pass subcommand args (everything after command)
	subArgs := []string{}
	if len(argsWithoutGlobal) > 2 {
		subArgs = argsWithoutGlobal[2:]
	}
    
    cfg, created, err := loadOrInitConfig(globalConfigPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if created {
		fmt.Printf("Config template created at %s. Please edit it and re-run.\n", globalConfigPath)
		return
	}

	switch cmd {
        case "read":
            handleRead(cfg, subArgs)
        case "header":
            handleHeader(cfg, subArgs)
        case "import":
            handleImport(cfg, subArgs)
        case "export":
            handleExport(cfg, subArgs)
        default:
            fmt.Printf("Unknown command: %s\n\n", cmd)
            printUsage()
	}
}

func handleRead(cfg *Config, args []string) {
	readCmd := flag.NewFlagSet("read", flag.ExitOnError)
	dbcName := readCmd.String("name", "", "DBC file name (without extension)")
	readCmd.StringVar(dbcName, "n", "", "DBC file name (shorthand)")
	record := readCmd.Int("record", 0, "Sample record index to display")
	readCmd.IntVar(record, "r", 0, "Sample record index (shorthand)")
	writeOut := readCmd.Bool("out", false, "Rebuild and write DBC to export directory")
	readCmd.BoolVar(writeOut, "o", false, "Rebuild and write DBC (shorthand)")

	readCmd.Parse(args)

	if *dbcName == "" {
		fmt.Println("Error: --name/-n is required for read")
		readCmd.Usage()
		return
	}
    
	dbc, meta, err := ReadDBCFile(*dbcName, cfg)
	if err != nil {
		log.Fatalf("Failed to read DBC: %v", err)
	}
    
    if dbc.Header.RecordCount <= uint32(*record) {
        log.Fatalf("Sample record index too large, records in file: 0-%d", dbc.Header.RecordCount-1)
    }

	fmt.Printf("Read %s:\n", meta.File)
	fmt.Printf("Record %d sample:\n", *record)
	PrintRecord(dbc.Records[*record], meta, dbc.StringBlock)

	if *writeOut {
		outPath := filepath.Join(cfg.Paths.Export, meta.File)
		if err := WriteDBC(dbc, meta, outPath); err != nil {
			log.Fatalf("Failed to rebuild DBC: %v", err)
		}
		fmt.Printf("\n%s written to %s\n", meta.File, outPath)
	}
}

func handleHeader(cfg *Config, args []string) {
	headerCmd := flag.NewFlagSet("header", flag.ExitOnError)
	dbcName := headerCmd.String("name", "", "DBC file name")
	headerCmd.StringVar(dbcName, "n", "", "DBC file name (shorthand)")
	headerCmd.Parse(args)

	if *dbcName == "" {
		fmt.Println("Error: --name/-n is required for header")
		headerCmd.Usage()
		return
	}

	header, err := ReadDBCHeader(*dbcName, cfg)
	if err != nil {
		log.Fatalf("Failed to read DBC header: %v", err)
	}

	fmt.Printf("Header info for %s:\n", *dbcName)
	fmt.Printf("  Magic: %s\n", string(header.Magic[:]))
	fmt.Printf("  Record Count: %d\n", header.RecordCount)
	fmt.Printf("  Field Count: %d\n", header.FieldCount)
	fmt.Printf("  Record Size: %d bytes\n", header.RecordSize)
	fmt.Printf("  String Block Size: %d bytes\n", header.StringBlockSize)
}

func handleImport(cfg *Config, args []string) {
	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
    dbcName := importCmd.String("name", "", "DBC file name")
	importCmd.StringVar(dbcName, "n", "", "DBC file name (shorthand)")
	importCmd.Parse(args)

	dbcDB, err := openDB(cfg.DBC)
	if err != nil {
		log.Fatalf("Failed to connect to DBC DB: %v", err)
	}
	defer dbcDB.Close()

    if *dbcName == "" {
        if err := ImportDBCs(dbcDB, cfg); err != nil {
            log.Fatalf("Export failed: %v", err)
        }
    } else {
        metaPath := filepath.Join(cfg.Paths.Meta, *dbcName+".meta.json")
        if err := ImportDBC(dbcDB, cfg, metaPath); err != nil {
            log.Fatalf("Export failed for %s: %v", *dbcName, err)
        }
    }

	log.Println("Import completed successfully!")
}

func handleExport(cfg *Config, args []string) {
	exportCmd := flag.NewFlagSet("export", flag.ExitOnError)
    dbcName := exportCmd.String("name", "", "DBC file name")
	exportCmd.StringVar(dbcName, "n", "", "DBC file name (shorthand)")
    force := exportCmd.Bool("force", false, "Force export even if versioning is enabled")
	exportCmd.BoolVar(force, "f", false, "Force export (shorthand)")
	exportCmd.Parse(args)

    if *force {
		cfg.Options.UseVersioning = false
	}

	dbcDB, err := openDB(cfg.DBC)
	if err != nil {
		log.Fatalf("Failed to connect to DBC DB: %v", err)
	}
	defer dbcDB.Close()

    if *dbcName == "" {
        if err := ExportDBCs(dbcDB, cfg); err != nil {
            log.Fatalf("Export failed: %v", err)
        }
    } else {
        metaPath := filepath.Join(cfg.Paths.Meta, *dbcName+".meta.json")
        if err := ExportDBC(dbcDB, cfg, metaPath); err != nil {
            log.Fatalf("Export failed for %s: %v", *dbcName, err)
        }
    }

	log.Println("Export completed successfully!")
}

func printUsage() {
	fmt.Println("Usage: dbcreader <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  read    - Read a DBC file and optionally rebuild it")
	fmt.Println("  header  - Print header info of a DBC file")
	fmt.Println("  import  - Import DBC files into the database")
	fmt.Println("  export  - Export database tables back to DBC files")
	fmt.Println("\nUse 'dbcreader <command> -h' for command-specific options")
}
