#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${DIR}

bin/voltdb create --deployment=deployment.xml --host=172.24.2.200 -B
