module Infra

  module Helpers

    def load_config
      return unless File.exist?('params.json')
      JSON.parse(File.read('params.json'))
    end

    def set_logger(logger)
      @logger = logger
    end

    def cleanup(*args)
      ['SOURCE_DIR', 'ESTORE_OUT_DIR', 'ESTORE_IN_DIR', 'GOODDATA_DIR'].each do |name|
        directory = get(name)
        FileUtils.rm_rf("#{directory}/.", :secure => true)
      end
    end

    def load_event_store
      run_shell("set -a; source workspace.prm; es -l load --basedir=./estore")
    end

    def truncate_event_store(from=get('LAST_SUCCESFULL_FINISH'))
      # TODO: Fix the error here. It uses wrong date
      if from
        run_shell("set -a; source workspace.prm; es -l truncate --basedir=./estore --timestamp=#{from}")
      else
        logger.warn "Variable LAST_SUCCESFULL_FINISH not filled in not truncating"
      end
    end

    def extract_event_store
      run_shell("set -a; source workspace.prm; es -l extract --basedir=./estore --extractdir=./estore")
    end

    def upload_data_with_cl(options = {})
      run_shell("#{get('CLTOOL_EXE')} -u#{get('LOGIN')} -p#{get('PASSWORD')} #{get('CL_SCRIPT')}")
    end

    def run_clover_graph(graph, options={})
      java_options = options[:java]
      java_params = java_options.nil? ? "" : "- #{java_options}"
      clover_options = options[:clover]
      command = "#{get('CLOVER_HOME')}/clover.sh #{get('CLOVER_PARAMS')} #{get('GRAPH_DIR') + graph} #{java_params}"
      run_shell(command)
    end

    def run_shell(command)
      logger.info "Running external command '#{command}'"
      cat = 'ruby -e"  ARGF.each{|line| STDOUT << line}  "'
      pid, stdin, stdout, stderr = Open4::popen4("sh")
      stdin.puts command
      stdin.close
      _, status = Process::waitpid2(pid)
      output = ""
      stdout.each_line do |line|
        output += line
      end
      $stdout.puts(output)
      logger.info(output)

      error = ""
      stderr.each_line do |line|
        error += line
      end
      $stderr.puts(error)
      logger.warn(error)

      if status.exitstatus == 0 
        logger.info "Finished external command '#{command}'"
      else
        logger.error "External command '#{command}' FAILED"
        fail "External step"
      end
      [output.chomp, status.exitstatus]
    end
  end
end