# CWL-metrics client

[CWL-metrics](https://github.com/inutano/cwl-metrics) is a framework to collect and analyze computational resource usage of workflow runs based on the [Common Workflow Language (CWL)](https://www.commonwl.org). CWL-metrics client offers an easy access to the elasticsearch server that stores metrics and workflow metadata.

# How to use

Easiest way to access your elasticsearch server of CWL-spec is to use `cwl-metrics fetch` command bundled in the [CWL-metrics](https://github.com/inutano/cwl-metrics). See README in the repo for details.

Docker container is also available via [quay.io](https://quay.io/repository/inutano/cwl-metrics-client).

```
$ cwl-metrics status
cwl-metrics is running.
$ docker run -it --rm -e ES_HOST=$ES_HOST -e ES_PORT=$ES_PORT quay.io/inutano/cwl-metrics-cleint:latest json
```
