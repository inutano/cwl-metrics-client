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
        wf["steps"].each_pair do |cid, step|
          tifs = total_input_file_size(step)
          table << [
            # ID
            cid[0..11],
            step["stepname"],
            # Env
            step["platform"]["hostname"],
            step["platform"]["instance_type"],
            # Metrics
            (step["metrics"]["cpu_total_percent"] if step["metrics"]),
            (step["metrics"]["memory_max_usage"]  if step["metrics"]),
            (step["metrics"]["memory_cache"]      if step["metrics"]),
            (step["metrics"]["blkio_total_bytes"] if step["metrics"]),
            # Container info
            step["container"]["process"]["image"],
            step["elapsed_sec"],
            step["container"]["process"]["exit_code"],
            # Container meta
            step["tool_status"],
            tifs,
            # Workflow meta
            wf["workflow_id"],
            wf["workflow_name"],
            wf["workflow_elapsed_sec"],
          ]
        end
      end
      table.map{|line| line.join("\t") }.join("\n")
    end

    def total_input_file_size(step)
      a = []
      step["inputs"].each do |input_name,input_value|
        if input_value.class == Array
          input_value.map do |val|
            if val.class == Hash && val["class"] == "File"
              a << val["size"]
            end
          end
        elsif input_value.class == Hash && input_value["class"] == "File"
          a << input_value["size"]
        end
      end
      a.reduce(:+)
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
      fetched = fetch_metrics_records
      extract_workflow_info.map do |wf|
        wf["steps"].each_key do |cid|
          wf["steps"][cid]["metrics"] = fetched[cid]
        end
        wf
      end
    end

    #
    # Fetch container metrics for all cids
    #
    def fetch_metrics_records
      bucket_num = bucket_size['aggregations']['bucket_num']['value']
      buckets = fetch_metrics(bucket_num)['aggregations']['summary_per_container_id']['buckets']
      Hash[
        buckets.map{|bucket|
          [
            bucket['key'],
            {
              "cpu_total_percent" => bucket['max_cpu_usage']['value'],
              "memory_max_usage"  => bucket['max_memory_usage']['value'],
              "memory_cache"      => bucket['max_memory_cache_usage']['value'],
              "blkio_total_bytes" => bucket['total_blkio']['value']
            }
          ]
        }
      ]
    end

    def bucket_size
      @@client.search(bucket_size_query)
    end

    def bucket_size_query
      {
        index: 'metrics',
        body: {
          query: {
            bool: {
              must: {
                match_all: {}
              },
              filter: {
                bool: {
                  should: [
                    {
                      term: {
                        'name.keyword': 'docker_container_cpu'
                      }
                    }
                  ]
                }
              }
            }
          },
          aggs: {
            summary_per_container_id: {
              terms: {
                field: 'fields.container_id.keyword',
                size: 1
              },
              aggs: {
                max_cpu_usage: {
                  max: {
                    field: 'fields.usage_percent'
                  }
                }
              }
            },
            bucket_num: {
              value_count: {
                field: 'fields.container_id.keyword'
              }
            }
          },
          size: 0
        }
      }
    end

    def fetch_metrics(bucket_num)
      @@client.search(fetch_metrics_query(bucket_num))
    end

    def fetch_metrics_query(bucket_num)
      {
        index: 'metrics',
        body: {
          query: {
            bool: {
              must: {
                match_all: {}
              },
              filter: {
                bool: {
                  should: [
                    {
                      term: {
                        'name.keyword': 'docker_container_cpu'
                      }
                    },
                    {
                      term: {
                        'name.keyword': 'docker_container_mem'
                      }
                    },
                    {
                      term: {
                        'name.keyword': 'docker_container_blkio'
                      }
                    }
                  ]
                }
              }
            }
          },
          aggs: {
            summary_per_container_id: {
              terms: {
                field: 'fields.container_id.keyword',
                size: bucket_num
              },
              aggs: {
                max_cpu_usage: {
                  max: {
                    field: 'fields.usage_percent'
                  }
                },
                max_memory_usage: {
                  max: {
                    field: 'fields.max_usage'
                  }
                },
                max_memory_cache_usage: {
                  max: {
                    field: 'fields.cache'
                  }
                },
                total_blkio: {
                  max: {
                    field: 'fields.io_service_bytes_recursive_total'
                  }
                }
              }
            }
          },
          size: 0
        }
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
      window_search(search_query, 0, 0)["total"]["value"].to_i
    end

    def window_search(search_query, from, size)
      q = search_query
      q[:body][:from] = from
      q[:body][:size] = size
      @@client.search(q)["hits"]
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
          "workflow_id" => record["_id"],
          "workflow_cwl_file" => wf_meta["cwl_file"],
          "workflow_start_date" => start_date,
          "workflow_end_date" => end_date,
          "workflow_elapsed_sec" => elapsed_sec,
          "inputs" => wf_meta["inputs"],
          "outputs" => wf_meta["outputs"],
          "steps" => extract_step_info(record["_source"]["steps"]),
        }
      end
    end

  	def search_workflows
      get_all_hits(search_workflows_query)
    end

    def search_workflows_query
      {
        index: "workflow",
        body: {
          query: {
            bool: {
              must: { "match_all": {} }
            }
          }
        }
      }
    end

    def extract_step_info(steps_hash)
      step_info = {}
      steps_hash.each_pair do |k,v|
        start_date = DateTime.parse(v["start_date"])
        end_date = DateTime.parse(v["end_date"])
        elapsed_sec = ((end_date - start_date) * 24 * 60 * 60).to_f

        step_info[v["container"]["process"]["id"]] = {
          "stepname" => v["stepname"],
          "start_date" => start_date,
          "end_date" => end_date,
          "elapsed_sec" => elapsed_sec,
          "step_cwl_file" => v["cwl_file"],
          "tool_status" => v["tool_status"],
          "inputs" => v["inputs"],
          "outputs" => v["outputs"],
          "container" => v["container"],
          "platform" => v["platform"],
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
