require 'infra'

describe Infra::App do

  before :each do
    app = App.new({
      :logger => Logger.new(STDOUT),
      :params => {
        :param1 => "1",
        :param1 => "2"
      }
    })

    app.step(:download) do
      puts "donwload"
    end

    app.step(:etl, :restartable => true) do
      puts "etl"
    end

    app.step :upload do
      puts "Upload"
    end
    
    @app = app
    
    
    
    broken_app = App.new({
      :logger => Logger.new(STDOUT)
    })

    broken_app.step(:download) do
      puts "download"
    end

    broken_app.step(:etl, :restartable => true) do
      fail
      puts "etl"
    end

    broken_app.step :upload do
      puts "Upload"
    end
    
    @broken_app = broken_app
  end

  it "should initialize" do
  
  end

  # it "Available steps should be provided in order" do
  #   steps = @app.steps.map {|step| step.name}
  #   steps.should == [:download, :etl, :upload]
  #   
  #   @app.sequence = [:upload, :download]
  #   
  #   steps = @app.steps.map {|step| step.name}
  #   steps.should == [:upload, :download]
  # end
  # 
  # it "Restartable steps should be in order" do
  #   steps = @app.restartable_steps.map {|step| step.name}
  #   steps.should == [:download, :etl]
  # end
  # 
  # it "should run all available steps if everything goes ok" do
  #   @app.run
  #   ran_steps = @app.steps.find_all {|step| step.ran}
  #   ran_steps.count.should == 3
  # end
  # 
  # it "should not ran following tasks if one goes down (by fail)" do
  #   @broken_app.run
  #   ran_steps = @broken_app.steps.find_all {|step| step.ran}
  #   finished_steps = @broken_app.steps.find_all {|step| step.finished}
  # 
  #   ran_steps.count.should == 2
  #   finished_steps.count.should == 1
  # end
  # 
  # it "should reset it state" do
  #   @app.run
  #   @app.steps.any? {|step| step.ran}.should == true
  #   @app.reset_steps
  #   @app.steps.any? {|step| step.ran}.should == false
  # end
  # 
  # it "Should tell it ran after run" do
  #   @app.run
  #   @app.ran.should == true
  # end
  # 
  # it "Should return the first available restartable task" do
  #   @broken_app.run
  #   @broken_app.propose_restart_point.name.should == :etl
  # end
  # 
  # it "should return only steps after download" do
  #   
  #   etl_step = @app.step_by_name(:etl)
  # end

  

end