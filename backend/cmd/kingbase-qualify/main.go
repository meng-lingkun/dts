package main

import (
	"os"
	"dts/backend/internal/domain"
	"dts/backend/internal/qualification/pgderivative"
)

func main() { os.Exit(pgderivative.Run(domain.DataSourceKingbase, os.Args[1:])) }
