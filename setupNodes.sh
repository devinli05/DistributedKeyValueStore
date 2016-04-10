#!/bin/bash
go run node.go 3 0 0 >pro00.log 2>&1  &
sleep 5
go run node.go 3 1 0 >pro01.log 2>&1  &
go run node.go 3 2 0 >pro02.log 2>&1  &
go run node.go 3 3 0 >pro03.log 2>&1  &
go run node.go 3 4 0 >pro04.log 2>&1  &
go run node.go 3 5 0 >pro05.log 2>&1  &
go run node.go 3 6 0 >pro06.log 2>&1  &
go run node.go 3 7 0 >pro07.log 2>&1  &
