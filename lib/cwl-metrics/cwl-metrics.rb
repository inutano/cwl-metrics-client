#
# cwl-metrics.rb: module for cwl-metirics-client
#
require 'json'

module CWLMetrics
  module CWLMetricsMethods
    def register_client(client)
      @@client = client
    end

    def json
      JSON.dump(aggregate)
    end

    def aggregate
      {
        "metrics": extract_workflow_info
      }
    end

    def extract_workflow_info
      search_workflows.map do |record|
        wf_meta = record["_source"]["workflow"]
        {
          "workflow_id": record["_id"],
          "workflow_name": wf_meta["cwlfile"],
          "platform": {
            "instance_type": wf_meta["platform"]["instance_type"],
            "region": wf_meta["platform"]["region"],
            "hostname": wf_meta["platform"]["hostname"],
          },
          "steps": extract_step_info(record["_source"]["steps"]),
        }
      end
    end

  	def search_workflows
      @@client.search(search_workflows_query)["hits"]["hits"]
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

    def extract_step_info(steps_hash)
      step_info = {}
      steps_hash.each_pair do |k,v|
        step_info[v["container_id"]] = {
          "stepname": v["stepname"],
          "container_name": v["container_name"],
          "tool_version": v["tool_version"],
          "tool_status": v["tool_status"],
          "input_files": extract_input_file_size(v)
        }
      end
      step_info
    end

    def extract_input_file_size(step_hash)
      input_files = {}
      step_hash.each_pair do |k,v|
        if v.class == Hash # go inside "input_files"
          v.each_pair do |input_file, input_value| # find input variables passed as File
            if input_value.class == Hash && input_value.has_key?("size")
              input_files[input_value["basename"]] = input_value["size"]
            end
          end
        end
      end
      input_files
    end
  end
  extend CWLMetricsMethods
end
