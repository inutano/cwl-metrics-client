#
# Dockerfile for CWL-metrics Elasticsearch client
#
FROM ruby:2.5.1-stretch
WORKDIR /
RUN apt-get update -y && apt-get install -y git jq
RUN git clone https://github.com/inutano/cwl-metrics-client
RUN cd /cwl-metrics-client && bundle install
ENTRYPOINT ["ruby", "/cwl-metrics-client/lib/client.rb"]
