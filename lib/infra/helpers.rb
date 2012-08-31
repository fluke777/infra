require 'pony'
require 'es'
require 'gd'
require 'archiver'
require 'sfdc_tests'

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
        if (name.directory?) then
         FileUtils::cd(name) do
            files = Dir.glob('*')
            files.each do |file|
              FileUtils.rm_rf(file, :secure => true)
            end
         end
        end
      end
    end

    def load_event_store(options={})
      basedir = options[:basedir] || get('ESTORE_DIR')
      pid = get('PID')
      es_name = get('ES_NAME')

      connect_to_gooddata()
      Es::Commands.load({
        :pid      => pid,
        :es_name  => es_name,
        :basedir  => basedir,
        :pattern  => "gen_load*.json",
        :only     => options[:only],
        :logger   => logger
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

      connect_to_gooddata()

      if from
        Es::Commands.truncate({
          :pid => pid,
          :es_name => es_name,
          :entity => entity,
          :timestamp => from,
          :basedir => basedir,
          :basedir_pattern => "gen_load*.json",
          :logger => logger
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

      fail "Please provide estore name as ES_NAME param in you params.json" if es_name.nil? || es_name.empty?

      connect_to_gooddata()

      Es::Commands.extract({
        :basedir    => basedir,
        :extractdir => extractdir,
        :pid        => get('PID'),
        :es_name    => get('ES_NAME'),
        :logger     => logger
      })
    end

    def upload_data_with_cl(options = {})
      login         = options[:login] || get('LOGIN')
      password      = options[:password] || get('PASSWORD')
      script_path   = options['script'] || get('CL_SCRIPT')
      host          = options[:cl_host] || get('CL_HOST')

      fail ArgumentError.new("Error in Upload_data_with_cl helper. Please define login either as parameter LOGIN in params.json or as :login option") if login.nil? || login.empty?
      fail ArgumentError.new("Error in Upload_data_with_cl helper. Please define login either as parameter PASSWORD in params.json or as :password option") if password.nil? || password.empty?

      run_shell("#{get('CLTOOL_EXE')} -u#{login} -p#{password} #{optional("-h",host)} #{script_path}")
    end

    def run_clover_graph(graph, options={})
      graph_path = get('GRAPH_DIR') + graph
      fail ArgumentError.new("Graph #{graph_path} does not exist") unless File.exist?(graph_path)
      java_options = options[:java_options]
      java_params = java_options.nil? ? "" : "- #{java_options}"
      clover_options = options[:clover]
      command = "set -a; source ./workspace.sh;#{get('CLOVER_EXE')} #{get('CLOVER_PARAMS')} #{graph_path} #{java_params}"
      run_shell(command)
    end

    def run_shell(command)

      logger.info "Running external command '#{command}'"

      output = nil
      error  = nil

      status = Open4::popen4("sh") do |pid, stdin, stdout, stderr|
        stdin.puts command
        stdin.close
        output = stdout.read
        $stdout.puts(output)
        error = stderr.read
        $stderr.puts(error)
      end

      logger.info(output)
      logger.warn(error)

      if status.exitstatus == 0
        logger.info "Finished external command '#{command}'"
        [output.chomp, status.exitstatus]
      else
        logger.error "External command '#{command}' FAILED"
        fail "External step"
      end
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
        :save_to            => (get('PROJECT_DIR') + "validations/sf_results").to_s,
        :mail_path          => (get('PROJECT_DIR') + "validations/out" + "mail.txt").to_s,
        :splunk_path        => (get('PROJECT_DIR') + "validations/out" + "splunk.txt").to_s,
        :json_path          => (get('PROJECT_DIR') + "validations/out" + "json.json").to_s,
        :pid                => pid,
        :rforce_connection  => rforce_connection,
        :project            => GoodData.project,
        :ms_project_name    => "#{customer}-#{project}"
      }
      
      d = GoodData::SfdcTests::ReportDownloader.new(options)
      reports = d.get_reports_for_validation
      
      d.get_and_save_sfdc_reports(reports)
    end

    def report_validations(options={})
      customer    = get('CUSTOMER')
      project     = get('PROJECT')
      pid         = get('PID')
      sfdc_login  = get('SFDC_USERNAME')
      sfdc_pass   = get('SFDC_PASSWORD')
      email_from  = options[:email_from] || "sf-validations@gooddata.com"
      email_to    = options[:email_to] || "ps-etl+clover@gooddata.com"
      
      fail "SFDC password is not defined" if sfdc_pass.nil?
      fail "SFDC login is not defined" if sfdc_login.nil?
      
      connect_to_gooddata
      GoodData.project = pid
      
      rforce_connection = RForce::Binding.new 'https://www.salesforce.com/services/Soap/u/20.0'
      rforce_connection.login sfdc_login, sfdc_pass
      
      options = {
        :save_to            => (get('PROJECT_DIR') + "validations/sf_results").to_s,
        :mail_path          => (get('PROJECT_DIR') + "validations/out" + "mail.txt").to_s,
        :splunk_path        => (get('PROJECT_DIR') + "validations/out" + "splunk.txt").to_s,
        :json_path          => (get('PROJECT_DIR') + "validations/out" + "json.json").to_s,
        :pid                => pid,
        :rforce_connection  => rforce_connection,
        :project            => GoodData.project,
        :ms_project_name    => "#{customer}-#{project}"
      }
      
      d = GoodData::SfdcTests::ReportDownloader.new(options)
      r = GoodData::SfdcTests::Runner.new(d, options)
      r.generate_example_group(options[:project])
      
      result = r.run ["--format", "EmailFormatter", "-o", options[:mail_path], "--format", "SplunkFormatter", "-o", options[:splunk_path], "--format", "JsonFormatter", "-o", options[:json_path]]
      
      result_subject = result == 0 ? "OK" : "ERROR"
    
      Pony.mail(:to => email_to, :from => email_from, :subject => "#{result_subject} - #{customer}/#{project} Validation", :body => "See attachment for details", :attachments => {"mail.txt" => File.read(options[:mail_path])})
      
      File.open('/mnt/log/gdc-clover', 'a') do |f|
        f.write File.read(options[:splunk_path])
      end
    end


    def sync_domain_with_csv(filename, options={})
      pid = options[:pid] || get('PID')
      domain = options[:domain] || get('GD_DOMAIN')

      fail "Please specify domain. Either as a :domain param in helper sync_domain_with_csv or as a GD_DOMAIN in your params.json" if domain.nil? || domain.empty?

      connect_to_gooddata(options)
      Gd::Commands::create_users_from_csv(filename, pid, domain)
    end

    def sync_domain_with_sf(options={})
      pid = options[:pid] || get('PID')
      login     = options[:sf_login] || get('SFDC_USERNAME')
      password  = options[:sf_password] || get('SFDC_PASSWORD')
      domain = options[:domain] || get('GD_DOMAIN')

      fail "Please specify domain. Either as a :domain param in helper sync_domain_with_csv or as a GD_DOMAIN in your params.json" if domain.nil? || domain.empty?
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

      fail "Please specify domain. Either as a :domain param in helper sync_domain_with_csv or as a GD_DOMAIN in your params.json" if domain.nil? || domain.empty?

      connect_to_gooddata
      Gd::Commands::sync_users_in_project_from_sf(login, password, pid, domain, options)

    end

    def sync_users_in_project_from_csv(file_name, options)
      pid = options[:pid] || get('PID')
      domain = options[:domain] || get('GD_DOMAIN')

      fail "Please specify domain. Either as a :domain param in helper sync_domain_with_csv or as a GD_DOMAIN in your params.json" if domain.nil? || domain.empty?

      connect_to_gooddata
      Gd::Commands::sync_users_in_project_from_csv(file_name, pid, domain, options)
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
      #   :file_list          => ["file.txt","file2.txt"]
      # }
      options[:source_dir] = get('SOURCE') if options[:source_dir].nil?
      options[:target_dir] = get('SOURCE_DIR') if options[:target_dir].nil?
      GDC::Downloader.download(options)
    end

    def download_from_sftp(options={})
      options[:server]          = get('SFTP_SERVER')
      options[:password]        = get('PASSWORD')
      options[:login]           = get('LOGIN')
      options[:pid]             = get('PID')
      options[:target_dir]      = get('SOURCE_DIR')
      options[:pattern]         = "*.zip" if options[:pattern].nil?
      GDC::SftpDownloader.download(options)
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
        :es_name  => get('ES_NAME'),
        :pattern  => "gen_load*.json",
        :logger   => logger
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
    def get_updated(definitions)
      to        = Time.now.utc
      login     = get('SFDC_USERNAME')
      password  = get('SFDC_PASSWORD')
      client = Salesforce::Client.new(login, password)

      params = {}

      definitions.each do |module_def|
        module_name = module_def[:module]
        fields      = module_def[:fields]

        param_name = "LAST_UPDATED_RECORD_#{module_name.upcase}"
        from = get(param_name).nil? ? nil : Time.parse(get(param_name))
        file_name = get('ESTORE_IN_DIR') + "#{module_name}.csv"
        answer = client.download_updated(:module => module_name, :fields => fields, :start_time => from, :end_time => to, :output_file => file_name)

        pp answer
        params[module_name] =  {
          param_name => answer
        }
      end
      params
    end

    def archive_to_s3
      bucket_name = "gooddata_com_#{get('CUSTOMER')}_#{get('PROJECT')}"
      GDC::Archiver.archive({
        :source_dir          => get('SOURCE_DIR'),
        :store_to_s3         => true,
        :logger              => logger,
        :bucket_name         => bucket_name,
        :s3_credentials_file => "/mnt/ms/.s3cfg"
      })

    end

    def connect_to_gooddata(options={})
      login     = options[:login]
      password  = options[:password]
      pid       = options[:pid]
      logger    = options[:logger] || Logger.new(get('PROJECT_DIR') + 'log' + 'http.log', 'daily')
      GoodData.logger = logger

      login = login || get('LOGIN')
      password = password || get('PASSWORD')
      begin
        GoodData.connect(login, password, nil, {
          :timeout => 0
        })
        GoodData.connection.connect!
      rescue RestClient::BadRequest => e
        fail "Seems like login or password for GoodData is wrong. Please verify LOGIN and PASSWORD params in params.json"
      end
    end

    def download_from_SFDC(timestamp=nil)
      logger.info "SFDC downloader helper START"
      time = nil
      unless timestamp.nil?
        begin
          time = Time.at(timestamp)
        rescue => e
          fail "Timestamp in helper download_from_SFDC is not nil but it cannot be converteinto valid time"
        end
      end

      is_incremental = get('SFDC_INCREMENTAL_DOWNLOAD')
      fail "SFDC_INCREMENTAL_DOWNLOAD has to be \"true\" or \"false\" in params.json" if is_incremental != "true" && is_incremental != "false"

      if time.nil? && is_incremental == "true"
        begin
          time = Time.at(get('LAST_FULL_RUN_START'))
        rescue => e
          fail "You are trying to do an incremental upload but the LAST_FULL_RUN_START is not set. This looks like you have never run the app before. Consider doing full upload of data."
        end
      end

      if time.nil? && is_incremental == "false"
        begin
          time = Date.strptime(get('DATA_BIRTHDAY'), '%Y-%m-%d').to_time
        rescue
          fail "DATA_BIRTHDAY has wrong format (should have '%Y-%m-%d') or is not set in params.json"
        end
      end

      logger.info("Downloader started. Downloading from: #{time.utc}")
      set("SFDC_DOWNLOAD_START_DATE", time.utc.iso8601)
      run_shell("#{get('PROJECT_DIR') + 'downloader/bin/download.sh'} #{get('PROJECT_DIR') + 'workspace.prm'}")
      logger.info("SFDC downloader END")
    end

    def mail_to_pager_duty(step = "", message = "")
      customer    = get('CUSTOMER')
      project     = get('PROJECT')
      hostname    = `hostname`.chomp 
      last_step   = steps.detect {|i| i.ran && !i.finished}
      step        = (last_step ? last_step.name : "unknown") if (step.nil? || step.empty?)
      message     = (@last_exception ?  @last_exception.inspect : "unknown") if (message.nil? || message.empty?)
      
      mail(:to => "clover@gooddata.pagerduty.com", :from => 'root@gooddata.com', :subject => "#{hostname}: #{customer} - #{project} ETL error", :body => "Error occured during #{step} step. Error: #{message}")
    end




  end
end