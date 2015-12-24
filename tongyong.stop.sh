#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

pidfile=logstash.pid

if [ ! -f ${pidfile} ]; then
    echo "Pid file does not exist, check if the logstash is running please."
    exit 1
fi

pid=`cat ${pidfile}`

ps -ef | grep $pid | grep -v grep | grep start.sh > /dev/null

if [ $? -eq 0 ]; then
    echo "run command: kill $pid"
    `kill $pid`
    rm $pidfile
else
    echo "The pid does not exist."
fi

exit 0
