#!/bin/bash
ssh -T $USER@lin16.ugrad.cs.ubc.ca >lin16.log 2>&1 <<- 'ENDSSH'
cd $PROJPATH
go run node.go 3 6 &
ENDSSH
