module dbctool

go 1.22.0

require(
    github.com/go-sql-driver/mysql v1.9.3
    filippo.io/edwards25519 v1.1.0
)

replace github.com/go-sql-driver/mysql => ../dep/mysql
replace filippo.io/edwards25519 => ../dep/edwards25519