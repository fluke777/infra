require 'pony'

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
      list = if args.empty?
        ['SOURCE_DIR', 'ESTORE_OUT_DIR', 'ESTORE_IN_DIR', 'GOODDATA_DIR'].map {|n| get(n)}
      else
        args
      end
      list.each do |name|
        FileUtils.rm_rf("#{name}/.", :secure => true)
      end
    end

    def load_event_store(options={})
      basedir = options[:basedir] || get('ESTORE_DIR')
      run_shell("set -a; source workspace.prm; /mnt/ms/bin/es -l load --basedir=#{basedir}")
    end

    def truncate_event_store(options={})
      basedir = options[:basedir] || get('ESTORE_DIR')
      from = options[:from] || get('LAST_FULL_RUN_START')

      if from
        run_shell("set -a; source workspace.prm; /mnt/ms/bin/es -l truncate --basedir=#{basedir} --timestamp=#{from}")
      else
        logger.warn "Variable LAST_SUCCESFULL_FINISH not filled in not truncating"
      end
    end

    def extract_event_store(options={})
      basedir = options[:basedir]       || get('ESTORE_DIR')
      extractdir = options[:extractdir] || get('ESTORE_DIR')
      run_shell("set -a; source workspace.prm; /mnt/ms/bin/es -l extract --basedir=#{basedir} --extractdir=#{extractdir}")
    end

    def upload_data_with_cl(options = {})
      login = options[:login] || get('LOGIN')
      pass = options[:password] || get('PASSWORD')
      script_path = options['script'] || get('CL_SCRIPT')

      fail ArgumentError.new("Error in Upload_data_with_cl helper. Please define login either as parameter LOGIN in params.json or as :login option") if login.nil? || login.empty?
      fail ArgumentError.new("Error in Upload_data_with_cl helper. Please define login either as parameter PASSWORD in params.json or as :password option") if password.nil? || password.empty?

      run_shell("#{get('CLTOOL_EXE')} -u#{login} -p#{password} #{script_path}")
    end

    def run_clover_graph(graph, options={})
      graph_path = get('GRAPH_DIR') + graph
      fail ArgumentError.new("Graph #{graph_path} does not exist") unless File.exist?(graph_path)
      java_options = options[:java_options]
      java_params = java_options.nil? ? "" : "- #{java_options}"
      clover_options = options[:clover]
      command = "#{get('CLOVER_EXE')} #{get('CLOVER_PARAMS')} #{graph_path} #{java_params}"
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

    def download_validations
      customer    = get('CUSTOMER')
      project     = get('PROJECT')
      pid         = get('PID')
      sfdc_login  = get('SFDC_USERNAME')
      sfdc_pass   = get('SFDC_PASSWORD')

      fail "SFDC password is not defined" if sfdc_pass.nil?
      fail "SFDC login is not defined" if sfdc_login.nil?

      GoodData.connect(get('LOGIN'), get('PASSWORD'))
      GoodData.project = pid

      rforce_connection = RForce::Binding.new 'https://www.salesforce.com/services/Soap/u/20.0'
      rforce_connection.login sfdc_login, sfdc_pass

      options = {
        :save_to            => "./validations/sf_results/#{customer}/#{project}",
        :mail_path          => "./validations/out/#{customer}/#{project}/mail.txt",
        :splunk_path        => "./validations/out/#{customer}/#{project}/splunk.txt",
        :json_path          => "./validations/out/#{customer}/#{project}/json.json",
        :pid                => pid,
        :rforce_connection  => rforce_connection,
        :project            => GoodData.project,
        :ms_project_name    => "#{customer}-#{project}"
      }

      d = GoodData::SfdcTests::ReportDownloader.new(options)
      reports = d.get_reports_for_validation
      # if cmd_options[:verbose]
      #   reports.each {|r| puts r.title}
      # end
      d.get_and_save_sfdc_reports(reports)
    end

    def add_users(options={})
      
    end

    # mail(:to => email, :from => 'sf-validations@gooddata.com', :subject => "SUBJ", :body => "See attachment for details")
    def mail(options={})
      options.merge!({
        # :via => :smtp
      })
      begin
        Pony.mail(options)
      rescue
        logger.warn "Email could not be sent"
      end
    end

  end
end