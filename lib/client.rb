#
# client.rb: elasticsearch client for CWL-metrics
#
$LOAD_PATH << __dir__

require 'elasticsearch'
require 'cwl-metrics/cwl-metrics'

ES_HOST = ENV["ES_HOST"] || "localhost"
ES_PORT = ENV["ES_PORT"] || "9200"

if __FILE__ == $0
  client = Elasticsearch::Client.new(hosts: "#{ES_HOST}:#{ES_PORT}", log: false)
  CWLMetrics.register_client(client)
  case ARGV.first
  when "json"
    puts CWLMetrics.json
  when "tsv"
    puts CWLMetrics.tsv
  end
end
