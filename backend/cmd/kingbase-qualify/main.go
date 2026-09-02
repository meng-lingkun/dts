package main

import (
	"os"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/qualification/pgderivative"
)

func main() { os.Exit(pgderivative.Run(domain.DataSourceKingbase, os.Args[1:])) }
