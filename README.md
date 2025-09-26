# DBCTool

`DBCTool` is a command-line utility for working with **World of Warcraft
DBC files**.\
It lets you inspect, import, and export DBCs to/from a MySQL database
using flexible JSON meta definitions.

------------------------------------------------------------------------

## ✨ Features

-   **Read DBC files**: inspect records and headers directly from the
    binary file.
-   **Rebuild DBCs**: output cleaned or modified versions of a DBC.
-   **Import**: load DBC files into MySQL tables (schema is generated
    from metadata).
-   **Export**: rebuild DBC files from MySQL tables, preserving field
    definitions.
-   **Configurable**: paths and database connection are set via a simple
    `config.json`.

------------------------------------------------------------------------

## 📦 Installation

Clone the repository and build with Go:

``` bash
git clone https://github.com/foereaper/dbctool.git --recurse
cd dbctool
go build -C src -o dbctool
```

You need **Go 1.20+** and access to a **MySQL server**.

------------------------------------------------------------------------

## ⚙️ Configuration

When run for the first time, `DBCTool` will generate a template
`config.json`:

``` json
{
  "dbc": {
    "user": "root",
    "password": "password",
    "host": "127.0.0.1",
    "port": "3306",
    "name": "dbc"
  },
  "paths": {
    "base": "../dbc_files",
    "export": "../dbc_export",
    "meta": "../meta"
  }
}
```

-   **dbc**: database connection details.
-   **paths.base**: directory containing original DBC files.
-   **paths.export**: output folder for rebuilt/exported DBCs.
-   **paths.meta**: directory with `*.meta.json` files describing each
    DBC's schema.

------------------------------------------------------------------------

## 🚀 Usage

General syntax:

``` bash
dbctool <command> [options]
```

### Commands

-   **read** --- Read a DBC file and optionally rebuild it

    ``` bash
    dbctool read --name=Spell --record=5 --out
    ```

    Options:

    -   `--name, -r` : DBC file name without extension (required).
    -   `--record, -i` : record index to display.
    -   `--out, -o` : rebuild and write the DBC to export directory.

-   **header** --- Show header info of a DBC

    ``` bash
    dbctool header --name=Spell
    ```

    Options:

    -   `--name, -r` : DBC file name without extension (required).

-   **import** --- Import all DBCs into the database

    ``` bash
    dbctool import
    ```

    Options:

    -   `--name, -r` : DBC file name without extension (optional), imports only this DBC.

-   **export** --- Export all tables back into DBC files

    ``` bash
    dbctool export
    ```

    Options:

    -   `--name, -r` : DBC file name without extension (optional), exports only this DBC.

### Global options

-   `--config=path/to/config.json` : override path to config file.\
    Can be placed before or after the subcommand.

------------------------------------------------------------------------

## 📂 Meta files

Each DBC requires a corresponding `*.meta.json` file in the `meta`
directory.\
These describe field types, primary/unique keys, and sort order.
Example:

``` json
{
  "file": "DBCName.dbc",
  "primaryKeys": ["ID"],
  "uniqueKeys": [["ID"]],
  "sortOrder": [{ "name": "ID", "direction": "ASC" }],
  "fields": [
    { "name": "ID", "type": "int32" },
    { "name": "Name", "type": "string" },
    { "name": "Description", "type": "Loc" }
  ]
}
```

------------------------------------------------------------------------

## 📜 License

MIT License © 2025 --- \[DBCTool\]
