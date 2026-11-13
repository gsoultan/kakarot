module github.com/gsoultan/kakarot/examples/orders

go 1.27.4

require (
	github.com/google/uuid v1.6.0
	github.com/gsoultan/kakarot v0.0.0
	github.com/gsoultan/kakarot/data v0.0.0
	github.com/gsoultan/storm v1.12.1
)

require (
	github.com/gsoultan/kakarot/cmd/kakarot v0.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.13.0 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/tools v0.52.0 // indirect
)

replace github.com/gsoultan/kakarot => ../..

tool github.com/gsoultan/kakarot/cmd/kakarot

replace github.com/gsoultan/kakarot/data => ../../data

replace github.com/gsoultan/kakarot/cmd/kakarot => ../../cmd/kakarot
