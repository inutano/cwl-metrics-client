#
# client.rb: elasticsearch client for CWL-metrics
#
$LOAD_PATH << __dir__

require 'elasticsearch'
require 'cwl-metrics/cwl-metrics'

if __FILE__ == $0
  endpoint = "35.173.197.179:9200"
  client = Elasticsearch::Client.new(hosts: endpoint, log: false)
  puts CWLMetrics.aggregate(client)
end
