#!/bin/bash
ssh -T $USER@lin17.ugrad.cs.ubc.ca >lin17.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 7 1 &
ENDSSH
