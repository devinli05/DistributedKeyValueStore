#!/bin/bash
ssh -T $USER@lin13.ugrad.cs.ubc.ca >lin13.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 3 0 &
ENDSSH
