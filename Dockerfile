#
# Dockerfile for CWL-metrics Elasticsearch client
#
FROM ruby:2.7.1
WORKDIR /
RUN apt-get update -y && apt-get install -y git jq
COPY . /app
RUN cd /app && bundle install
ENTRYPOINT ["ruby", "/app/lib/client.rb"]
