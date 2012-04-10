require 'logger'
require 'terminal-table'
require 'json'
require 'time'
require 'active_support/time'
require 'action_view'
require 'rainbow'
require 'open4'

class StepError < RuntimeError
  
  attr_accessor :action
  
  def initialize(message, action)
    super(message)
    @action = action
  end
end

class ExitException < RuntimeError
end


logger = Logger.new(STDOUT)

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

    attr_accessor :logger, :error, :sequence, :ran, :last_start, :last_finish

    def initialize(options = {})
      @sequence = [:download, :es_load, :es_extract, :etl, :upload, :sync_users, :validation]
      @step_blocks = {}
      @logger = options[:logger]
      @error = false
      @ran = false
      
      @parameters = {
        "param1" => "value1"
      }
    end

    def get(key)
      @parameters[key]
    end

    def set(key, value)
      logger.info("Prameter '#{key}' was set to value '#{value}'")
      @parameters[key] = value
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
      

      # fail StepError.new("failed with status #{$?}", name) if $? != 0
      # logger.info "Step #{name} Ended"
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
          :last_start => last_start,
          :last_finish => last_finish,
          :error  => error,
          :steps  => steps.map {|step| {
            :name     => step.name,
            :ran      => step.ran,
            :finished => step.finished
          }}
        }
      }
      File.open('setup.json', 'w') do |f|
        f.write(JSON.pretty_generate(data))
      end
    end

    def awake
      return unless File.exist?('setup.json')
      data = JSON.parse(File.read('setup.json'), :symbolize_names => true)
      @ran = data[:application][:ran]
      @last_start = data[:application][:last_start]
      @last_finish = data[:application][:last_finish]
      @error = data[:application][:error]
      data[:application][:steps].each do |step|
        s = step_by_name(step[:name])
        # binding.pry
        s.ran = step[:ran]
        s.finished = step[:finished]
      end
    end

    def run_downloader
      puts "running downloader"
    end

    def run_archiver
      puts "running archiver"
    end

    def run_clover
      puts "runnning clover"
    end

    def run_shell(command)
      logger.info "Running external command '#{command}'"
      pid, stdin, stdout, stderr = Open4::popen4(command)
      _, status = Process::waitpid2(pid)
      stdout.each_line do |line|
        logger.info(line)
      end
      stderr.each_line do |line|
        logger.warn(line)
      end
      if status.exitstatus == 0 
        logger.info "Finished external command '#{command}'"
      else
        logger.error "External command '#{command}' FAILED"
        fail "External step"
      end
    end

    private
    def run_steps(steps_to_run)
      @last_start = Time.now.to_i
      steps_to_run.each do |step|
        step.ran = true
        instance_eval(&step.block)
        break if @error
        step.finished = true
      end
      @last_finish = Time.now.to_i
      sleep
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
      duration = @app.last_finish - @app.last_start
      table = Terminal::Table.new(:title => "Last Run Summary".bright) do |t|
        t << ["Finished Sucessfully", convert_value(!@app.error)]
        t << ["Started", "#{distance_of_time_in_words(now - @app.last_start)} ago"]
        t << ["Finished", "#{distance_of_time_in_words(now - @app.last_finish)} ago"]
        t << ["Started", Time.at(@app.last_start).to_s]
        t << ["Finished", Time.at(@app.last_finish).to_s]
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
