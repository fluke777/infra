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

include FileUtils

module WorkspaceConstants
  PROJECT_DIR = Pathname.new(".").expand_path
  DATA_DIR    = PROJECT_DIR + 'data'
  CLTOOL_HOME = Pathname.new("/mnt/tools/cltool/bin")
  SCRIPT_DIR  = PROJECT_DIR + "script"
  
  DEFAULT_PARAMS = {
    "PROJECT_DIR"     => PROJECT_DIR,
    "LOG_PATH"        => PROJECT_DIR + "logs",
    "CONFIG_DIR"      => PROJECT_DIR + "config",
    "ESTORE_DIR"      => PROJECT_DIR + "estore",
    "GRAPH_DIR"       => PROJECT_DIR + "graph",
    
    "META_DIR"        => PROJECT_DIR + "meta",

    "CLOVER_HOME"     => Pathname.new("/mnt/tools/clover"),
    "SCRIPT_HOME"     => Pathname.new("/mnt/tools/script"),
    
    "DATA_DIR"        => DATA_DIR,
    "SOURCE_DIR"      => DATA_DIR + "source",
    "ESTORE_IN_DIR"   => DATA_DIR + "estore-in",
    "ESTORE_OUT_DIR"  => DATA_DIR + "estore-out",
    "TRANSFORM_DIR"   => DATA_DIR + "transform",
    "GOODDATA_DIR"    => DATA_DIR + "gooddata",
    "LOOKUP_DIR"      => DATA_DIR + "lookup",
    "TEMP_DIR"        => DATA_DIR + "temp",
    
    "SCRIPT_DIR"      => SCRIPT_DIR,
    "CL_SCRIPT"       => SCRIPT_DIR + 'gd_load.script',
    "CLTOOL_HOME"     => CLTOOL_HOME,
    "CLTOOL_EXE"      => CLTOOL_HOME + 'gd.sh',
    
    "CLOVER_PARAMS"   => "-nodebug -loglevel ERROR -logcfg /Users/fluke/sandbox/clover/log4j.properties -cfg #{PROJECT_DIR}/workspace.prm",
    
    "PID"             => File.read(PROJECT_DIR + 'pid')
  }
end

class StepError < RuntimeError
  
  attr_accessor :action
  
  def initialize(message, action)
    super(message)
    @action = action
  end
end

class ExitException < RuntimeError
end

module Infra

  class Step

    attr_accessor :restartable, :block, :name, :ran, :finished

    def initialize(name, options={}, &b)
      @ran = false
      @finished = false
      @name = name
      @block = b
      @restartable = options[:restartable]
    end

  end

  class App

    attr_accessor :logger, :error, :sequence, :ran, :last_successful_start, :last_successful_finish, :last_attempt

    include Infra::Helpers

    def initialize(options = {})
      @sequence = [:clean_up, :download, :preformat, :pre_es_transform, :es_load, :es_extract, :transform, :upload, :sync_users, :validation]
      @step_blocks = {}
      @logger = options[:logger]
      @error = false
      @ran = false
      
      @parameters = {}
      @saved_parameters = {}
    end

    def get(key)
      return if @parameters.nil?
      @parameters[key]
    end

    def set(key, value, options={})
      @parameters = {} if @parameters.nil?
      logger.info("Prameter '#{key}' was set to value '#{value}'") unless options[:silent]
      @parameters[key] = value
      interpolate_workspace
    end

    def save(key, value)
      @saved_parameters = {} if @saved_parameters.nil?
      @saved_parameters[key] = value
      set(key, value)
    end

    def set_logger(logger)
      @logger = logger
    end

    def step(name, options={}, &b)
      fail "You need to define a block for action #{name}" unless block_given?

      wrapper = Proc.new do
        logger.info "Step started #{name}"
        begin
          result = instance_eval(&b)
        rescue ExitException
          Kernel.exit
        rescue StandardError => e
          logger.error e.inspect
          @error = true
        ensure
          if @error then
            logger.error("Step finished #{name} with error")
          else
            logger.info("Step finished #{name}")
          end
        end
      end

      s = Step.new(name, options, &wrapper)
      @step_blocks[name.to_s] = s
    end

    def load_config
      return unless File.exist?('params.json')
      JSON.parse(File.read('params.json'))
    end

    def initialize_params
      params = load_config()
      merged_params = WorkspaceConstants::DEFAULT_PARAMS.merge(params)
      merged_params.each_pair do |key, val|
        set(key, val, :silent => true)
      end
    end

    def interpolate_workspace
      File.open('workspace.prm', 'w') do |f|
        @parameters.each_pair do |key, val|
          f.puts "#{key}=#{val}"
        end
      end
    end

    def exit
      logger.info "Exit from inside of step"
      raise ExitException.new
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
      @ran = true
      @error = false
      run_steps(steps)
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
      logger.info "Restarted from last checkpoint #{s.name}"
      restart_from_step(s)
    end

    def sleep
      data = {
        :application => {
          :ran    => ran,
          :last_successful_start => last_successful_start,
          :last_attempt => last_attempt,
          :last_successful_finish => last_successful_finish,
          :error  => error,
          :steps  => steps.map {|step| {
            :name     => step.name,
            :ran      => step.ran,
            :finished => step.finished
          }}
        },
        :params => @saved_parameters
      }
      File.open('setup.json', 'w') do |f|
        f.write(JSON.pretty_generate(data))
      end
    end

    def awake
      unless File.exist?('setup.json')
        logger.warn("setup.json does not exist")
        return
      end
      data = JSON.parse(File.read('setup.json'), :symbolize_names => true)
      
      if data[:application].nil?
        logger.warn("setup.json exists but it is probably empty")
        return
      else
        @ran = data[:application][:ran]
        @last_successful_start      = data[:application][:last_successful_start]
        @last_attempt               = data[:application][:last_attempt]
        @last_successful_finish     = data[:application][:last_successful_finish]

        set('LAST_SUCCESFULL_FINISH', @last_successful_finish, :silent => true)
        set('LAST_ATTEMPT', @last_attempt, :silent => true)
        set('LAST_SUCCESSFUL_START', @last_successful_start, :silent => true)

        data[:params] && data[:params].each_pair do |key, val|
          set(key.to_s, val)
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
    def run_steps(steps_to_run)
      fail "ETL is already runnning" if File.exist?('running.pid')
      begin
        FileUtils.touch('running.pid')
        @last_attempt = Time.now.to_i
        steps_to_run.each do |step|
          step.ran = true
          instance_eval(&step.block)
          break if @error
          step.finished = true
        end
        if !@error
          @last_successful_finish = Time.now.to_i
          @last_successful_start = @last_attempt
        end
        sleep
      ensure
        FileUtils.rm_f('running.pid')
      end
    end

    def restart_from_step(restart_step)
      steps_to_run = steps_from(restart_step)
      @ran = true
      @error = false
      run_steps(steps_to_run)
    end

  end

  class AppGUI

    include ActionView::Helpers::DateHelper

    def initialize(app)
      @app = app
    end

    def print_table(title, table, header="")
      header = ["Index"] + header unless header.empty?
      index = 1
      table = Terminal::Table.new(:title => title, :headings => header) do |t|
        table.each do |line|
          t << [index] + convert_values(line)
          index += 1
        end
      end
      puts table
    end

    def steps
      print_table("Steps overview", @app.steps.map {|s| [s.name, s.ran, s.finished]}, ["Name", "Ran", "Finsihed"])
    end

    def ran_steps
      print_table("Ran steps", @app.ran_steps.map {|s| [s.name]})
    end

    def summary
      now = Time.now.to_i
      duration = @app.last_successful_finish - @app.last_successful_start
      table = Terminal::Table.new(:title => "Last Run Summary".bright) do |t|
        t << ["Finished Sucessfully", convert_value(!@app.error)]
        t << ["Started", "#{distance_of_time_in_words(now - @app.last_successful_start)} ago"]
        t << ["Finished", "#{distance_of_time_in_words(now - @app.last_successful_finish)} ago"]
        t << ["Started", Time.at(@app.last_successful_start).to_s]
        t << ["Finished", Time.at(@app.last_successful_finish).to_s]
        t << ["Duration", "#{duration} seconds"]
        t << ["Duration", "#{duration} seconds"]
        t << ["Implemented Steps", @app.steps.count]
        t << ["Steps Ran", @app.ran_steps.count]
        t << ["Steps Finished", @app.ran_finished.count]
        t << :separator
        t << ["Steps", ""]
        t << :separator
        @app.steps.each do |step|
          name = step.name.to_s
          run_flag = ''
          run_flag = '*' if step.finished
          run_flag = '-' if !step.finished && step.ran
          
          step_flag = step.finished ? name.color(:green) : name
          
          t << [@app.steps.index(step) + 1, sprintf("[%1s %1s] %s", run_flag, step.restartable ? 'R' : '', step_flag)]
        end
      end
      puts table
    end

    private
    def convert_values(values)
      values.map do |val|
        convert_value(val)
      end
    end

    def convert_value(val)
      if val == true
        "Yes".color(:green)
      elsif val == false
        "No".color(:red)
      else
        val
      end
    end

  end

end