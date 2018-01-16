#!/bin/bash
sbt package && ./local-spark-submit $1
