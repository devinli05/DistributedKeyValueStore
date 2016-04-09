#!/bin/bash
ssh -T $USER@lin15.ugrad.cs.ubc.ca >lin15.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 5 &
ENDSSH
