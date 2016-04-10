#!/bin/bash
ssh -T $USER@lin12.ugrad.cs.ubc.ca >lin12.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 2 1 &
ENDSSH
