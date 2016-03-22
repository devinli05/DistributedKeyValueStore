#!/bin/bash
go run node.go 3 0 &
sleep 5
go run node.go 3 1 &
go run node.go 3 2 &
go run node.go 3 3 &
