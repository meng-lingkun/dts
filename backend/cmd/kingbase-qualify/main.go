package main

import (
	"dts/backend/internal/domain"
	"dts/backend/internal/qualification/pgderivative"
	"os"
)

func main() { os.Exit(pgderivative.Run(domain.DataSourceKingbase, os.Args[1:])) }
