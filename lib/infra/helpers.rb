require 'pony'
require 'es'
require 'gd'

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
      pid = get('PID')
      es_name = get('ES_NAME')

      Es::Commands.load({
        :pid => pid,
        :es_name => es_name,
        :basedir => basedir,
        :pattern => "gen_load*.json"
      })
    end

    def truncate_event_store(options={})
      basedir = options[:basedir] || get('ESTORE_DIR')
      from = options[:from] || get('LAST_FULL_RUN_START')
      entity = options[:entity]
      pid = get('PID')
      es_name = get('ES_NAME')

      fail "Please either specify entity to be truncated or specify :all parameter in helper truncate_event_store" if entity.nil?
      entity = nil if entity.to_s == "all"

      if from
        Es::Commands.truncate({
          :pid => pid,
          :es_name => es_name,
          :entity => entity,
          :timestamp => from,
          :basedir => basedir
        })
      else
        logger.warn "Variable LAST_FULL_RUN_START not filled in not truncating"
      end
    end

    def extract_event_store(options={})
      basedir = options[:basedir]       || get('ESTORE_DIR')
      extractdir = options[:extractdir] || get('ESTORE_DIR')
      pid = get('PID')
      es_name = get('ES_NAME')

      fail "Please provide estore name as ES_NAME param in you params.json" if es_name.nil? || es_name.empty

      Es::Commands.extract({
        :basedir    => basedir,
        :extractdir => extractdir,
        :pid        => get('PID'),
        :es_name    => get('ES_NAME')
      })
    end

    def upload_data_with_cl(options = {})
      login         = options[:login] || get('LOGIN')
      password      = options[:password] || get('PASSWORD')
      script_path   = options['script'] || get('CL_SCRIPT')

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

      connect_to_gooddata
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

    def sync_domain_with_csv(filename, options={})
      pid = options[:pid] || get('PID')
      domain = options[:domain]

      connect_to_gooddata(options)
      Gd::Commands::create_users_from_csv(filename, domain, pid)
    end

    def sync_domain_with_sf(options={})
      pid = options[:pid] || get('PID')
      login     = options[:sf_login] || get('SFDC_USERNAME')
      password  = options[:sf_password] || get('SFDC_PASSWORD')
      domain = options[:domain] || get('GD_DOMAIN')

      fail "Please specify sf login either as a parameter sf_login to helper sync_domain_with_sf or SFDC_USERNAME param" if login.nil? || login.empty?
      fail "Please specify sf password either as a parameter sf_password to helper sync_domain_with_sf or SFDC_PASSWORD param" if password.nil? || password.empty?
      
      connect_to_gooddata
      Gd::Commands::create_users_from_sf(login, password, pid, domain)
    end

    def sync_users_in_project_from_sf(options={})
      pid = options[:pid] || get('PID')
      login     = options[:sf_login] || get('SFDC_USERNAME')
      password  = options[:sf_password] || get('SFDC_PASSWORD')
      domain = options[:domain] || get('GD_DOMAIN')

      connect_to_gooddata
      Gd::Commands::sync_users_in_project_from_sf(login, password, pid, domain, options)

    end

    def sync_users_in_project_from_csv(file_name, options)
      pid = options[:pid] || get('PID')
      domain = options[:domain]

      connect_to_gooddata
      Gd::Commands::sync_users_in_project_from_csv(file_name, pid, domain, options={})
    end

    def execute_dml(dml)
      pid = get('PID')
      connect_to_gooddata
      GoodData.project = pid
      response = GoodData.post("/gdc/md/#{pid}/dml/manage", { 'manage' => { 'maql' => dml}})
      while (GoodData.get response['uri'])['taskState']['status'] != "OK"
        sleep(20)
      end
    end

    def download(options={})
      # {
      #   :pattern            => "data*.csv",
      #   :exclude_pattern    => "data3.csv",
      #   :source_dir         => "/Users/fluke/test_src",
      #   :target_dir         => "/Users/fluke/test_dst",
      #   :occurrence         => :true,
      #   :check_index        => "name.idx"
      # }
      GDC::Downloader.download(options)
    end

    def download_from_ftp(options={})
      fail "Not implemented"
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

    def load_deleted_records(options={})
      connect_to_gooddata
      Es::Commands::load_deleted(options.merge({
        :basedir  => get('ESTORE_DIR'),
        :pid      => get('PID'),
        :es_name  => get('ES_NAME')
      }))
    end

    # GET DELETED RECORDS. EXPERIMENTAL FEATURE
    def get_deleted_records(modules)
      to        = Time.now.utc
      login     = get('SFDC_USERNAME')
      password  = get('SFDC_PASSWORD')
      client = Salesforce::Client.new(login, password)

      params = {}

      modules.each do |module_name|
        param_name = "LAST_DELETED_RECORD_#{module_name.upcase}"
        from = get(param_name).nil? ? nil : Time.parse(get(param_name))
        file_name = get('ESTORE_IN_DIR') + "#{module_name}_deleted.csv"
        answer = client.get_deleted_records(:module => module_name, :startTime => from, :endTime => to, :output_file => file_name)
        pp answer
        params[param_name] = answer[1]
      end
      params
    end


    # GET UPDATED RECORDS. EXPERIMENTAL FEATURE
    def get_updated(modules)
      to        = Time.now.utc
      login     = get('SFDC_USERNAME')
      password  = get('SFDC_PASSWORD')
      client = Salesforce::Client.new(login, password)

      params = {}

      modules.each do |module_name|
        param_name = "LAST_DELETED_RECORD_#{module_name.upcase}"
        from = get(param_name).nil? ? nil : Time.parse(get(param_name))
        file_name = get('ESTORE_IN_DIR') + "#{module_name}_deleted.csv"
        answer = client.get_deleted_records(:module => module_name, :startTime => from, :endTime => to, :output_file => file_name)
        pp answer
        params[param_name] = answer[1]
      end
      params
    end

    def connect_to_gooddata(login=nil, password=nil, pid=nil)
      login = login || get('LOGIN')
      password = password || get('PASSWORD')
      begin
        GoodData.connect(login, password)
        GoodData.connection.connect!
      rescue RestClient::BadRequest => e
        fail "Seems like login or password for GoodData is wrong. Please verify LOGIN and PASSWORD params in params.json"
      end
    end

  end
end