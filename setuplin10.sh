#!/bin/bash
ssh -T $USER@lin10.ugrad.cs.ubc.ca >lin10.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 0 &
ENDSSH
