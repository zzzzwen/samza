#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

echo home_dir=$home_dir
echo "framework base (location of this script). base_dir=$base_dir"

if [ ! -d "$base_dir/lib" ]; then
  echo "Unable to find $base_dir/lib, which is required to run."
  exit 1
fi

HADOOP_YARN_HOME="${HADOOP_YARN_HOME:-$HOME/.samza}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_YARN_HOME/conf}"
CLASSPATH=$HADOOP_CONF_DIR
GC_LOG_ROTATION_OPTS="-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10241024"
DEFAULT_LOG4J_FILE=$base_dir/lib/log4j.xml
BASE_LIB_DIR="$base_dir/lib"
# JOB_LIB_DIR will be set for yarn container in ContainerUtil.java
# for others we set it to home_dir/lib
JOB_LIB_DIR="${JOB_LIB_DIR:-$home_dir/lib}"

export JOB_LIB_DIR=$JOB_LIB_DIR

for file in $BASE_LIB_DIR/*.[jw]ar;
  do
    if [[ $file == *"samza-core"* ]]; then
        CLASSPATH=$CLASSPATH:$file
    fi
  done


echo $CLASSPATH

CLASSPATH=$CLASSPATH rmiregistry 1999 &

echo "RMI listening to port 1999"