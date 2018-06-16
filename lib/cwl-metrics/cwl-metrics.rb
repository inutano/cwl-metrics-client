#
# cwl-metrics.rb: module for cwl-metirics-client
#
require 'json'
require 'date'

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
            # ID
            cid[0..11],
            step[:stepname],
            # Env
            wf[:platform][:hostname],
            wf[:platform][:instance_type],
            # Metrics
            step[:metrics][:cpu_total_percent],
            step[:metrics][:memory_max_usage],
            step[:metrics][:memory_cache],
            step[:metrics][:blkio_total_bytes],
            # Container info
            step[:docker_image],
            step[:docker_elapsed_sec],
            step[:docker_exit_code],
            # Container meta
            step[:tool_status],
            tifs,
            # Workflow meta
            wf[:workflow_id],
            wf[:workflow_name],
            wf[:workflow_elapsed_sec],
          ]
        end
      end
      table.map{|line| line.join("\t") }.join("\n")
    end

    def tsv_header
      [
        # ID
        "container_id",
        "stepname",
        # Env
        "hostname",
        "instance_type",
        # Metrics
        "cpu_total_percent",
        "memory_max_usage",
        "memory_cache",
        "blkio_total_bytes",
        # Container info
        "container_image",
        "container_elapsed_sec",
        "container_exit_code",
        # Container meta
        "tool_status",
        "total_inputfile_size",
        # Workflow meta
        "workflow_id",
        "workflow_name",
        "workflow_elapsed_sec",
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
      [
        metrics_cpu(cid),
        metrics_memory(cid),
        metrics_blkio(cid),
      ].inject(&:merge)
    end

    def metrics_cpu(cid)
      records = search_container_metrics(cid, "docker_container_cpu")
      usage_parcent_values = records.map{|r| r["_source"]["fields"]["usage_percent"] }.compact
      {
        "cpu_total_percent": usage_parcent_values.sort.last,
      }
    end

    def metrics_memory(cid)
      records = search_container_metrics(cid, "docker_container_mem")
      memory_usage_fields = records.map{|r| r["_source"]["fields"] }
      {
        "memory_max_usage": memory_usage_fields.map{|r| r["max_usage"] }.compact.sort.last,
        "memory_cache": memory_usage_fields.map{|r| r["cache"] }.compact.sort.last,
      }
    end

    def metrics_blkio(cid)
      records = search_container_metrics(cid, "docker_container_blkio")
      blkio_records = records.map{|r| r["_source"]["fields"]["io_service_bytes_recursive_total"] }.compact
      {
        "blkio_total_bytes": blkio_records.sort.last,
      }
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
    def search_container_metrics(cid, name)
      get_both_ends_of_hits(search_container_metrics_query(cid, name))
    end

    def search_container_metrics_query(cid, name)
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
                      term: {
                        "name": name
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
        start_date = DateTime.parse(wf_meta["start_date"])
        end_date = DateTime.parse(wf_meta["end_date"])
        elapsed_sec = ((end_date - start_date) * 24 * 60 * 60).to_f

        {
          "workflow_id": record["_id"],
          "workflow_name": wf_meta["cwl_file"],
          "workflow_start_date": start_date,
          "workflow_end_date": end_date,
          "workflow_elapsed_sec": elapsed_sec,
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
        d_inspect = v["docker_inspect"]
        start_date = DateTime.parse(d_inspect["start_time"])
        end_date = DateTime.parse(d_inspect["end_time"])
        elapsed_sec = ((end_date - start_date) * 24 * 60 * 60).to_f

        step_info[v["container_id"]] = {
          "stepname": v["stepname"],
          "container_name": v["container_name"],
          "tool_version": v["tool_version"],
          "tool_status": v["tool_status"],
          "input_files": extract_input_file_size(v),
          "docker_image": v["docker_image"],
          "docker_cmd": v["docker_cmd"],
          "docker_start_date": start_date,
          "docker_end_date": end_date,
          "docker_elapsed_sec": elapsed_sec,
          "docker_exit_code": d_inspect["exit_code"],
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
