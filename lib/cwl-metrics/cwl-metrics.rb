#
# cwl-metrics.rb: module for cwl-metirics-client
#
require 'json'

module CWLMetrics
  module CWLMetricsMethods
    def aggregate(client)
      container_id_mappings(client)
    end

    def container_id_mappings(client)
      search_workflows(client).map do |record|
        {
          "workflow_id": record["_id"],
          "workflow_name": record["_source"]["workflow"]["cwlfile"],
          "cid_mappings": steps_to_mapping(record["_source"]["steps"]),
        }
      end
    end

    def steps_to_mapping(steps_hash)
      mappings = {}
      steps_hash.each_pair do |k,v|
        mappings[k] = v["container_id"]
      end
      mappings
    end

  	def search_workflows(client)
      client.search(search_workflows_query)["hits"]["hits"]
    end

    def search_workflows_query
      {
        index: 'workflow',
        body: {
          query: {
            match: {
              "_type" => "workflow_log"
            }
          }
        }
      }
    end
  end
  extend CWLMetricsMethods
end
