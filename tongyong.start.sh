#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

nohup bin/logstash -f ./conf.d &
echo $! > logstash.pid
