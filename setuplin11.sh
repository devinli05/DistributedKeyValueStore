#!/bin/bash
ssh -T $USER@lin11.ugrad.cs.ubc.ca >lin11.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 1 &
ENDSSH
