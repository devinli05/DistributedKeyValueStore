#!/bin/bash
ssh $USER@lin10.ugrad.cs.ubc.ca "pkill -u $USER -f /tmp/go &"
ssh $USER@lin11.ugrad.cs.ubc.ca "pkill -u $USER -f /tmp/go &"
ssh $USER@lin12.ugrad.cs.ubc.ca "pkill -u $USER -f /tmp/go &"
ssh $USER@lin13.ugrad.cs.ubc.ca "pkill -u $USER -f /tmp/go &"
