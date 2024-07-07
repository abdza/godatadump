#!/bin/bash

# Set the target OS and architecture
export GOOS=windows
export GOARCH=amd64

# Build the executable
go build -o mssql-data-dumper.exe mssql-data-dumper.go
