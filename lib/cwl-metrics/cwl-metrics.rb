#
# cwl-metrics.rb: module for cwl-metirics-client
#
require 'json'

module CWLMetrics
  module CWLMetricsMethods
    def register_client(client)
      @@client = client
    end

    #
    # Output methods
    #

    def json
      JSON.dump({"CWL-metrics": metrics})
    end

    def tsv
      table = [tsv_header]
      metrics.each do |wf|
        wf[:steps].each_pair do |cid, step|
          tifs = step[:input_files].values.reduce(:+) if !step[:input_files].empty?
          table << [
            cid[0..11],
            step[:stepname],
            wf[:platform][:instance_type],
            step[:metrics][:cpu_total_percent],
            step[:metrics][:memory_max_usage],
            step[:metrics][:memory_cache],
            step[:metrics][:blkio_total_bytes],
            step[:metrics][:elapsed_time],
            wf[:workflow_id],
            wf[:workflow_name],
            wf[:workflow_start_date],
            wf[:workflow_end_date],
            step[:container_name],
            step[:tool_version],
            step[:tool_status],
            tifs,
          ]
        end
      end
      table.map{|line| line.join("\t") }.join("\n")
    end

    def tsv_header
      [
        "container_id",
        "stepname",
        "instance_type",
        "cpu_total_percent",
        "memory_max_usage",
        "memory_cache",
        "blkio_total_bytes",
        "elapsed_time",
        "workflow_id",
        "workflow_name",
        "workflow_start_date",
        "workflow_end_date",
        "container_name",
        "tool_version",
        "tool_status",
        "total_inputfile_size",
      ]
    end

    #
    # Summarize metrics for each workflow run
    #

    def metrics
      extract_workflow_info.map do |wf|
        wf[:steps].each_key do |cid|
          wf[:steps][cid][:metrics] = container_metrics(cid)
        end
        wf
      end
    end

    #
    # Metrics for each container
    #

    def container_metrics(cid)
      records = search_container_metrics(cid).map{|r| r["_source"] }
      {
        "cpu_total_percent": cpu_total_percent(records),
        "memory_max_usage": memory_max_usage(records),
        "memory_cache": memory_cache(records),
        "blkio_total_bytes": blkio_total_bytes(records),
        "elapsed_time": elapsed_time(records),
      }
    end

    def cpu_total_percent(records)
      extract_metrics_values(records, "docker_container_cpu", "usage_percent")
    end

    def memory_max_usage(records)
      extract_metrics_values(records, "docker_container_mem", "max_usage")
    end

    def memory_cache(records)
      extract_metrics_values(records, "docker_container_mem", "cache")
    end

    def blkio_total_bytes(records)
      extract_metrics_values(records, "docker_container_blkio", "io_service_bytes_recursive_total")
    end

    def elapsed_time(records)
      timestamps = records.map{|r| r["timestamp"] }.sort
      if timestamps.size > 1
        timestamps.last - timestamps.first
      elsif timestamps.size == 1
        timestamps.first
      end
    end

    def extract_metrics_values(records, name, field)
      records.select{|r| r["name"] == name }.map{|r| r["fields"][field] }.compact.sort.last
    end

    #
    # Methods to retrieve metrics data via Elasticsearch API
    #
    def search_window_width
      5000
    end

    def get_both_ends_of_hits(search_query)
      all_hits = get_all_hits(search_query).sort_by{|hit| hit["_source"]["timestamp"] }
      [
        all_hits.first(50),
        all_hits.last(50),
      ].flatten
    end

    def get_all_hits(search_query)
      total_hits = get_total_hits(search_query)
      hits_a = total_hits.times.each_slice(search_window_width).map do |i_a|
        window_search(search_query, i_a.first, search_window_width)["hits"]
      end
      hits_a.flatten
    end

    def get_total_hits(search_query)
      window_search(search_query, 0, 0)["total"].to_i
    end

    def window_search(search_query, from, size)
      q = search_query
      q[:body][:from] = from
      q[:body][:size] = size
      @@client.search(q)["hits"]
    end

    #
    # Get metrics data by container id
    #
    def search_container_metrics(cid)
      get_both_ends_of_hits(search_container_metrics_query(cid))
    end

    def search_container_metrics_query(cid)
      {
        index: 'telegraf',
        body: {
          query: {
            bool: {
              must: { match_all: {} },
              filter: {
                bool: {
                  must: [
                    {
                      term: {
                        "fields.container_id": cid,
                      },

                    },
                    {
                      terms: {
                        "name": ["docker_container_cpu", "docker_container_mem", "docker_container_blkio"]
                      },
                    }
                  ]
                }
              }
            }
          }
        }
      }
    end

    #
    # Retrieve workflow metadata from index:workflow
    #

    def extract_workflow_info
      search_workflows.map do |record|
        wf_meta = record["_source"]["workflow"]
        {
          "workflow_id": record["_id"],
          "workflow_name": wf_meta["cwl_file"],
          "workflow_start_date": wf_meta["start_date"],
          "workflow_end_date": wf_meta["end_date"],
          "platform": {
            "instance_type": wf_meta["platform"]["ec2_instance_type"],
            "region": wf_meta["platform"]["ec2_region"],
            "hostname": wf_meta["platform"]["hostname"],
            "total_memory": wf_meta["platform"]["total_memory"],
            "disk_size": wf_meta["platform"]["disk_size"],
          },
          "steps": extract_step_info(record["_source"]["steps"]),
        }
      end
    end

  	def search_workflows
      get_all_hits(search_workflows_query)
    end

    def search_workflows_query
      {
        index: 'workflow',
        body: {
          query: {
            bool: {
              must: { "match_all": {} },
              filter: {
                term:
                {
                  "_type": "workflow_log"
                }
              }
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
