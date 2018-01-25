#!/bin/bash
sbt package && ./local-spark-submit.sh $1
