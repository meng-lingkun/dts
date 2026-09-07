package main

import (
	"os"
	"dts/backend/internal/domain"
	"dts/backend/internal/qualification/pgderivative"
)

func main() { os.Exit(pgderivative.Run(domain.DataSourceOpenGauss, os.Args[1:])) }
