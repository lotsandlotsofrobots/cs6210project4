#!/bin/bash

# TEST ONE

killall mr_worker
killall mrdemo

rm config.ini
echo "TEST CASE 1 - Worker = 1, Reducers = 1 "

echo "n_workers=4" >> ./config.ini
echo "worker_ipaddr_ports=localhost:50051,localhost:50052,localhost:50053,localhost:50054" >> ./config.ini
echo "input_files=input/testdata_1.txt,input/testdata_2.txt,input/testdata_3.txt" >> ./config.ini
echo "output_dir=output" >> ./config.ini
echo "n_output_files=16" >> ./config.ini
echo "map_kilobytes=500" >> ./config.ini
echo "user_id=cs6210" >> ./config.ini

./mr_worker localhost:50051 > /dev/null &
./mrdemo > /dev/null

md5sum ./output/*

killall mr_worker
killall mrdemo

echo "Worker = 1, Reducers = 4 - Test Failed"
