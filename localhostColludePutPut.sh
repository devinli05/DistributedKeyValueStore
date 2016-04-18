#!/bin/bash
# collude to remove {something} at node 0 and put {something:val} at node 1
curl -H "Content-Type: application/json" -X POST -d '{"task":"something","description":"study 418"}' http://localhost:8880/add &
curl -H "Content-Type: application/json" -X POST -d '{"task":"something","description":"study 404"}' http://localhost:8881/add &
sleep 3
# get {something} at node 0 and 1
curl -H "Content-Type: application/json" -X GET http://localhost:8880/get/something
curl -H "Content-Type: application/json" -X POST -d '{"task":"something","description":"study 420"}' http://localhost:8880/add
curl -H "Content-Type: application/json" -X GET http://localhost:8880/get/something
