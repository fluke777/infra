require 'infra'

describe Infra::App do

  before :each do
    app = Infra::App.new({
      :logger => Logger.new(File.open('/dev/null', 'a')),
      :params => {
        :param1 => "1",
        :param2 => "2"
      }
    })

    app.step(:download) do
      puts "download"
    end

    app.step(:transform, :restartable => true) do
      puts "etl"
    end

    app.step :upload do
      puts "Upload"
    end

    @app = app

  end

  it "should initialize" do
    @app.should_not == nil
  end

  it "should be able to access params" do
    @app.get('param1').should == '1'
    @app.get(:param1).should == '1'
  end

  it "should be able to set params" do
    @app.set("a", "b")
    @app.get("a").should == "b"
  end

  it "should return nil even if parameters were not set in app" do
    app = Infra::App.new()
    app.get('foo').should == nil
  end

  it "should be able to save param. This behaves the same way as set + it should be persisted" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.save('foo', 'bar')
    app.get('foo').should == 'bar'
    app.saved_parameters.has_key?('foo').should == true
    app.saved_parameters['foo'].should == 'bar'
  end

  it "should be able to define steps" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step :download do
      puts "Downloading"
    end
  end

  it "should be able to work with defined steps" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step :download do
      puts "Downloading"
    end
    app.step :transform do
      puts "ETL"
    end
    app.steps.count.should == 2
  end

  it "should warn you if you define a step that will not be run" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    lambda do
      app.step :non_existing_step
    end.should(raise_error)
  end

  it "should be able to specify restartable steps" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step :download
    app.step :transform, :restartable => true

    app.steps.count.should == 2
    app.restartable_steps.count.should == 2
  end

  it "should include first step in restartables even when it is not marked" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step :download
    app.step :transform, :restartable => true

    app.restartable_steps.include?(app.steps.first)
  end

  it "can provide a step referenced by name" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step :download

    step = Infra::Step.new(:transform) do
      puts "transform"
    end
    app.add_step(step)

    app.step_by_name(:transform).should == step
  end

  it "should return the steps as ran after they run" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    app.step(:transform, :restartable => true) {}

    app.run
    app.ran_steps.count.should == 2
  end

  it "should handle gracefully when a step fails" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    app.step(:preformat, :restartable => true) { fail }
    app.step(:transform, :restartable => true) { }
    app.run
  end

  it "should not run more steps after one fails" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    first_step = Infra::Step.new(:download) {fail}
    second_step = Infra::Step.new(:transform) {}
    app.add_step(first_step)
    app.add_step(second_step)

    first_step.should_receive(:run).and_raise(StandardError)
    second_step.should_not_receive(:run)
    app.run

  end

  it "should not mark not run steps as run" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    app.step(:preformat, :restartable => true) { fail }
    app.step(:transform, :restartable => true) { }

    app.run
    app.ran_steps.count.should == 2
    app.ran_finished.count.should == 1
  end

  it "should propose a restart point" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    app.step(:preformat, :restartable => true) { fail }
    app.step(:transform) { }

    app.run

    app.propose_restart_point.should == app.step_by_name(:preformat)
  end

  it "should add a step via add_step method" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    step = Infra::Step.new(:download) {}
    app.add_step(step)
    app.steps.count.should == 1
  end

  it "should call the blocks in step" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))

    step = Infra::Step.new(:download) {}

    step.should_receive(:run)
    app.add_step(step)
    app.run
  end

  it "should return steps from named step" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    
    app.sequence.each {|name| app.step(name) {}}
    step = app.step_by_name(:preformat)
    app.steps_from(step).map {|step| step.name.to_sym }.should == [:preformat, :pre_es_transform, :es_load, :es_extract, :transform, :upload, :sync_users, :validation]
  end

  it "should run only steps that were not already ran" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:es_load, :restartable => true) {}
    app.step(:transform) { fail }

    app.run
    app.ran_steps.count.should == 7
    restart_point = app.propose_restart_point
    restart_point.should == app.step_by_name(:es_load)

    # fix the transformation
    app.step(:transform) { }

    app.step_by_name(:download).should_not_receive(:run)
    app.step_by_name(:pre_es_transform).should_not_receive(:run)
    restart_point.should_receive(:run)

    app.restart_from_last_checkpoint
    app.ran_steps.count.should == 10
  end

  it "should mark the app as ran when it is run" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}

    app.ran?.should == false
    app.run
    app.ran?.should == true
  end

  it "should mark as failed if it fails" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {fail}

    app.failed?.should == false
    app.run
    app.failed?.should == true
  end

  it "should not be marked as failed when I use exit to exit" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {exit}

    app.failed?.should == false
    app.run
    app.failed?.should == false
  end

  it "should mark the steps as ran when exited by user" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {exit}

    app.run
    app.ran_steps.count.should == 2
  end

  it "should mark the last step both as ran and finfished (as opposed to failed executions)" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {exit}

    app.run
    app.step_by_name(:download).ran?.should == true
    app.step_by_name(:download).finished?.should == true
  end

  it "should return vailable steps in order" do
    steps = @app.steps.map {|step| step.name}
    steps.should == ["download", "transform", "upload"]

    @app.sequence = [:upload, :download]

    steps = @app.steps.map {|step| step.name}
    steps.should == ["upload", "download"]
  end

  it "should initialize parameters" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.get('SCRIPT_DIR').should_not == nil
  end

  it "should change the last attempt if it fails" do
    time_now = Time.parse("Feb 24 2012")
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {fail}

    Timecop.travel(time_now) do
      app.run
      app.last_attempt.should == time_now.to_i
      app.last_successful_finish.should == nil
    end
  end

  it "should change the last attempt and last succesfull finish if it succeeds" do
    time_now = Time.parse("Feb 24 2012")
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}

    Timecop.freeze(time_now) do
      app.run
      app.last_attempt.should == time_now.to_i
      app.last_successful_start.should == app.last_attempt
      app.last_successful_finish.should_not == nil
    end
  end

  it "should change the last attempt and last succesfull finish if it exits" do
    time_now = Time.parse("Feb 24 2012")
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {exit}

    Timecop.freeze(time_now) do
      app.run
      app.last_attempt.should == time_now.to_i
      app.last_successful_start.should == app.last_attempt
      app.last_successful_finish.should_not == nil
    end
  end

  it "should tell it is partial run if it is restarted" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    
    app.run
    app.partial_run?.should == false
    app.full_run?.should == true
  end

  it "should tell it is full run if it is ran" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    
    app.restart_from_last_checkpoint
    app.partial_run?.should == true
    app.full_run?.should == false
  end

end