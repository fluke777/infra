require 'logger'
require 'terminal-table'
require 'json'
require 'time'
require 'action_view'
require 'active_support/time'
require 'rainbow'
require 'open4'
require 'fileutils'
require 'pathname'
require 'timecop'
require 'downloader'
require 'salesforce'
require 'psql_logger'
include FileUtils

# This is here because of bugs in active_support + builder + xs
class String
  def fast_xs_absorb_args(*args)
    fast_xs
  end
  alias_method :to_xs, :fast_xs_absorb_args
end


module Infra

  TOOLS_ROOT              = Pathname.new('/mnt/ms/tools')
  CHIPMUNK_ROOT           = TOOLS_ROOT + 'chipmunk'
  PROJECTS_TEMPLATE_ROOT  = CHIPMUNK_ROOT + 'template'
  PROJECTS_ROOT           = CHIPMUNK_ROOT + 'projects'

  class App

    attr_accessor :logger, :psql_logger, :error, :sequence, :ran, :last_successful_start, :last_successful_finish, :last_attempt, :bail, :saved_parameters, :last_full_run_start, :current_full_run_start, :is_production

    include Infra::Helpers

    def initialize(options = {})
      @sequence = [:clean_up, :download, :preformat, :pre_es_transform, :es_load, :es_extract, :post_es_format, :transform, :upload, :sync_users, :validation, :archive]
      @step_blocks = {}
      @logger = options[:logger]
      @psql_logger = options[:psql_logger]
      @error = false
      @ran = false
      @bail = false
      @ran_in_directory = options[:home_directory].nil? ? Pathname.new('.').expand_path : Pathname.new(options[:home_directory]).expand_path
      @run_params_file = options[:run_params_file]
      @workspace_filename = options[:workspace_file]
      @saved_parameters = {}
      @custom_params = options[:params] || {}
      @run_after_failure = []
      @run_after_success = []
      @is_production = !!options[:is_production]
      initialize_params
    end

    def failed?
      error
    end

    def ran?
      ran
    end

    def full_run?
      @full_run
    end

    def partial_run?
      @partial_run
    end

    def get(key)
      return if @parameters.nil?
      @parameters[key.to_s]
    end

    def optional(param,value)
      if (!value.nil?) && (!value.empty?) then
        return "#{param} #{value}"
      else
        return ""
      end
    end
    
    def set(key, value, options={})
      @parameters = {} if @parameters.nil?
      logger.info("Prameter '#{key}' was set to value '#{value}'") unless options[:silent]
      @parameters[key.to_s] = value
      write_workspace unless @workspace_filename.nil?
    end

    def save(key, value)
      @saved_parameters = {} if @saved_parameters.nil?
      @saved_parameters[key] = value
      set(key, value)
    end

    def add_step(step)
      fail "Step #{step.name} will not be run. Only steps #{@sequence.join(', ')} are supported." unless @sequence.include?(step.name.to_sym)
      logger.warn "Step with name '#{step.name}' is already defined" if @step_blocks.has_key?(step.name)
      @step_blocks[step.name] = step
    end

    def step(name, options={}, &b)
      s = Step.new(name, options, &b)
      add_step(s)
    end

    def initialize_params
        project_dir         = Pathname.new(@ran_in_directory).expand_path
        data_dir            = project_dir + 'data'
        cltool_home         = TOOLS_ROOT + "cltool/current/bin"
        script_dir          = project_dir + "script"
        clover_home         = TOOLS_ROOT + "clover"
        clover_current_home = clover_home + "current"
        source_dir          = data_dir + "source"
        
        default_params = {
          "PROJECTS_ROOT"   => PROJECTS_ROOT,
          "TOOLS_DIR"       => TOOLS_ROOT,
          "PROJECT_DIR"     => project_dir,
          "LOG_PATH"        => project_dir + "log",
          "LOG_DIR"        => project_dir + "log",
          "CONFIG_DIR"      => project_dir + "config",
          "ESTORE_DIR"      => project_dir + "estore",
          "GRAPH_DIR"       => project_dir + "graph",
                               
          "META_DIR"        => project_dir + "meta",
          "CLOVER_HOME"     => clover_current_home,
          "CLOVER_EXE"      => clover_current_home + 'clover.sh',
          
          "SCRIPT_HOME"     => TOOLS_ROOT + "script",
          
          "DATA_DIR"        => data_dir,
          "SOURCE_DIR"      => source_dir,
          "ESTORE_IN_DIR"   => data_dir + "estore-in",
          "ESTORE_OUT_DIR"  => data_dir + "estore-out",
          "TRANSFORM_DIR"   => data_dir + "transform",
          "GOODDATA_DIR"    => data_dir + "gooddata",
          "LOOKUP_DIR"      => data_dir + "lookup",
          "TEMP_DIR"        => data_dir + "temp",
          
          "SCRIPT_DIR"      => script_dir,
          "CL_SCRIPT"       => script_dir + 'gd_load.txt',
          "CLTOOL_HOME"     => cltool_home,
          "CLTOOL_EXE"      => cltool_home + 'gdi.sh',
          
          "CLOVER_PARAMS"   => "-nodebug -loglevel ERROR -logcfg #{clover_current_home + 'log4j.properties'} -cfg #{@workspace_filename}",
          "SFDC_DOWNLOAD_DIR" => source_dir,
          "INFRA_ENVIRONMENT" => @is_production ? "PRODUCTION" : "DEVELOPMENT"

        }
      @default_params = default_params
      merged_params = default_params.merge(@custom_params)
      merged_params.each_pair do |key, val|
        set(key, val, :silent => true)
      end
    end

    def write_workspace
      output = interpolate_workspace
      File.open(@workspace_filename, 'w') do |f|
        output.each {|o| f.puts o}
      end
      output = interpolate_workspace(:quoted => true)
      File.open('workspace.sh', 'w') do |f|
        output.each {|o| f.puts o}
      end
    end

    def interpolate_workspace(options={})
      quoted = options[:quoted] || false
      output = []
      @parameters.each_pair do |key, val|
        if quoted
          output << "#{key}=\"#{val.to_s.gsub('"', '\"')}\""
        else
          output << "#{key}=#{val}"
        end
      end
      output
    end

    def steps
      @step_blocks.values_at(*sequence.map {|s| s.to_s}).find_all {|step| step}
    end

    def restartable_steps
      restartables = steps.find_all {|step| step && step.restartable}
      first_step = steps.first
      if restartables.include?(first_step) then
        restartables
      else
        [first_step] + restartables
      end
    end

    def reset_steps
      steps.each do |step|
        step.finished = false
        step.ran = false
      end
    end

    def run
     reset_steps
     @full_run = true
     @partial_run = false
     @ran = true
     @error = false
     @bail = false
     @current_full_run_start = Time.now.to_i

     log_banner("New run started")
     log_to_psql("log_start")
     run_steps(steps)
    end

    def log_banner(message)
      logger.info ""
      logger.info ""
      l = message.length + 4
      logger.info "=" * l
      logger.info "= #{message} ="
      logger.info "=" * l
    end

    def step_by_name(name)
      @step_blocks[name.to_s]
    end

    def steps_from(restart_step)
      filter = false
      steps.find_all {|step| filter = true if step == restart_step; filter}
    end

    def ran_steps
      steps.find_all {|s| s.ran}
    end

    def ran_finished
      steps.find_all {|s| s.finished}
    end

    def propose_restart_point
      proposition = steps.first
      steps.each do |step|
          proposition = step if step.ran && step.restartable
      end
      proposition
    end
    
    def restart_from_last_checkpoint
      s = propose_restart_point
      log_banner("Restarted from last checkpoint - running from step \"#{s.name}\"")
      log_to_psql("log_start")
      restart_from_step(s)
    end

    def after_failure(obj=nil, &b)
      if obj
        @run_after_failure << obj
      elsif block_given?
        @run_after_failure << b
      end
    end

    def after_success(obj=nil, &b)
      if obj
        @run_after_success << obj
      elsif block_given?
        @run_after_success << b
      end
    end

    def sleep
      data = {
        :application => {
          :ran    => ran,
          :last_successful_start  => last_successful_start,
          :last_attempt           => last_attempt,
          :last_successful_finish => last_successful_finish,
          :last_full_run_start    => last_full_run_start,
          :current_full_run_start => current_full_run_start,
          :error  => error,
          :steps  => steps.map {|step| {
            :name     => step.name,
            :ran      => step.ran,
            :finished => step.finished
          }}
        },
        :params => @saved_parameters
      }
      unless @run_params_file.nil?
        File.open(@run_params_file, 'w') do |f|
          f.write(JSON.pretty_generate(data))
        end
      end
    end

    def awake
      unless File.exist?(@run_params_file)
        logger.warn("File for serializing state #{@run_params_file} does not exist")
        return
      end
      data = JSON.parse(File.read(@run_params_file), :symbolize_names => true)

      if data[:application].nil?
        logger.warn("setup.json exists but it is probably empty")
        return
      else
        @ran = data[:application][:ran]
        @last_successful_start      = data[:application][:last_successful_start]
        @last_attempt               = data[:application][:last_attempt]
        @last_successful_finish     = data[:application][:last_successful_finish]
        @last_full_run_start        = data[:application][:last_full_run_start]
        @current_full_run_start     = data[:application][:current_full_run_start]

        set('LAST_SUCCESFULL_FINISH', @last_successful_finish, :silent => true)
        set('LAST_ATTEMPT', @last_attempt, :silent => true)
        set('LAST_SUCCESSFUL_START', @last_successful_start, :silent => true)
        set('LAST_FULL_RUN_START',  @last_full_run_start, :silent => true)
        set('CURRENT_FULL_RUN_START', @current_full_run_start, :silent => true)

        @saved_parameters = {} if @saved_parameters.nil?
        data[:params] && data[:params].each_pair do |key, val|
          set(key.to_s, val, :silent => true)
          @saved_parameters[key] = val
        end

        @error = data[:application][:error]
        data[:application] && data[:application][:steps].each do |step|
          s = step_by_name(step[:name])
          # the step has configuration in json but such a step is not defined in app
          next if s.nil?
          s.ran = step[:ran]
          s.finished = step[:finished]
        end
      end
    end

    private
    def do_psql_log?
      @is_production && !@psql_logger.nil?
    end
    
    def log_to_psql(method, *params)
      begin
        @psql_logger.send(method, *params) if do_psql_log?
      rescue Exception => e
        logger.error("Log to postgres2 failed with message: #{e.message}")
        mail_to_pager_duty("log to psql", "Log to postgres2 failed with message: #{e.message}") unless !!@psql_error_sent
        @psql_error_sent = true
      end
    end
    
    def run_steps(steps_to_run)
      fail "ETL is already runnning" if File.exist?('running.pid')
      begin
        FileUtils.touch('running.pid')
        @last_attempt = Time.now.to_i
        steps_to_run.each do |step|
          step.ran = true
          run_step(step)
          break if @error
          step.finished = true
          break if @bail
        end
        if !@error
          @last_successful_finish = Time.now.to_i
          @last_successful_start  = @last_attempt
          @last_full_run_start    = @current_full_run_start
        end
        sleep
      ensure
        begin
          if @error
            @run_after_failure.each do |callback|
              callback.call(@last_exception)
            end
          else
            @run_after_success.each do |callback|
              callback.call
            end
            log_to_psql("log_end")
          end
        ensure
          FileUtils.rm_f('running.pid')
        end
      end
    end
    
    def run_step(step)
      logger.info "Step started #{step.name}"
      log_to_psql("log_step_start", step.name)
      write_workspace unless @workspace_filename.nil?
      begin
        result = step.run(self)
      rescue ArgumentError => e
        puts e.inspect.color(:red)
        puts e.backtrace
        fail e
      rescue SystemExit => e
        if e.status != 0
          @error = true
          @last_exception = e
        else
          @bail = true
        end
        logger.info "Exit from inside of step with code #{e.status}"
      rescue StandardError => e
        puts e.inspect.color(:red)
        puts e.backtrace
        logger.error e.backtrace
        logger.error e.inspect
        @error = true
        @last_exception = e
      ensure
        if @error then
          logger.error("Step finished #{step.name} with error")
          log_to_psql("log_error", step.name, @last_exception.message)
        else
          logger.info("Step finished #{step.name}")
          log_to_psql("log_step_end", step.name)
        end
      end
    end
    
    def restart_from_step(restart_step)
      steps_to_run = steps_from(restart_step)
      @ran = true
      @error = false
      @full_run = false
      @partial_run = true
      run_steps(steps_to_run)
    end

  end

end