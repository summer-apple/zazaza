#!/bin/bash
. /etc/profile
i=`ps -ef | grep point_tasks.py | grep -v grep | wc -l`
if [ $i -lt 1 ]
then
echo "`date +%Y-%m-%d-%H:%M:%S` restart point task"
cd /home/hadoop/spark/product
nohup /usr/local/env/python3/python /home/hadoop/spark/product/point_tasks.py &
fi
