#!/bin/bash
ssh -T $USER@lin14.ugrad.cs.ubc.ca >lin14.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 4 &
ENDSSH
