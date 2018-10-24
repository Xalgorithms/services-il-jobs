[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Build Status](https://travis-ci.org/Xalgorithms/services-il-jobs.svg?branch=master)](https://travis-ci.org/Xalgorithms/services-il-jobs)

This is a collection of Spark jobs that implement the [document
pipeline](https://github.com/Xalgorithms/general-documentation/blob/master/pipeline.md). The
rules use Cassandra, Kafka streams and MongoDB as sources of information. Rule
output is written to several different Kafka topics.

# Executing jobs

There are a few scripts in the project that help execute Spark jobs against a
Kubernetes or local Spark master. These scripts take care of specifying the
dependent JARs required by the jobs. These scripts read from configuration files
(just bash files that are sourced into the executing script) that provide
additional configuration per job and per deployment target. Each of the scripts
merely takes the name of the job to run:

```
# execute locally and show the log
$ ./local-spark-submit.sh <name>

# deploy to Kubernetes Spark master
$ ./deploy-kube.sh <name>
```

# Available jobs

* `ApplicableRules`: Runs a job that receives JSON input from Kafka that
  includes a reference to a document and a rule id. If this job determines that
  the rule is *applicable* to the document, it pushes the input data to the next
  queue in the pipeline.

* `ValidateApplicableRules`: Same as `ApplicableRules` expect that it the Kafka
  topics that the CLI uses for validation.

* `EffectiveRules`: Runs a job that receives JSON input from Kafka that includes
  a reference to a document. The job determines **all** of the rules that are
  *effective* for the document using the meta-data stored in Cassandra. The
  document reference and rule id are pushed to a Kafka topic that is subscribed
  to by the `ApplicableRules` job.

* `ValidateEffectiveRules`: Same as `EffectiveRules` expect that it the Kafka
  topics that the CLI uses for validation.




