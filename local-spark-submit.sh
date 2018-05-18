# Copyright (C) 2018 Don Kelly <karfai@gmail.com>
# Copyright (C) 2018 Hayk Pilosyan <hayk.pilos@gmail.com>

# This file is part of Interlibr, a functional component of an
# Internet of Rules (IoR).

# ACKNOWLEDGEMENTS
# Funds: Xalgorithms Foundation
# Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

# This program is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, either version 3 of
# the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public
# License along with this program. If not, see
# <http://www.gnu.org/licenses/>.
#!/bin/bash
echo "> attempting: $1"
OPTS=`cat $1.opts.development`
echo ">   additional opts: $OPTS"

spark-submit --class "org.xalgorithms.jobs.$1" --packages datastax:spark-cassandra-connector:2.0.3-s_2.11,com.typesafe:config:1.3.1,org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,net.ceedubs:ficus_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.2.0,org.gnieh:diffson-play-json_2.11:2.2.1 $OPTS target/scala-2.11/xa-spark-jobs_2.11-1.0.jar
