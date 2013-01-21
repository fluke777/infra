module Infra

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
      duration = @app.last_successful_finish && @app.last_successful_start && @app.last_successful_finish - @app.last_successful_start
      table = Terminal::Table.new(:title => "Last Run Summary".bright) do |t|
        t << ["Finished Sucessfully", convert_value(!@app.error)]
        t << ["Started", "#{distance_of_time_in_words(now - @app.last_successful_start)} ago"]
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

      table = Terminal::Table.new(:title => "Last Succesful Run Summary".bright) do |t|
        t << ["Started", "#{distance_of_time_in_words(now - @app.last_successful_start)} ago"]
        t << ["Finished", "#{distance_of_time_in_words(now - @app.last_successful_finish)} ago"]
        t << ["Started", Time.at(@app.last_successful_start).to_s]
        t << ["Finished", Time.at(@app.last_successful_finish).to_s]
        t << ["Duration", "#{duration} seconds"]
        t << ["Duration", "#{duration} seconds"]
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
  