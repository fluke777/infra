require 'infra'

describe Infra::App do

  it "should gracefully handle fails in data downloads" do
    introduce_bug = true
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.step(:download) {}
    app.step(:preformat) {fail if introduce_bug}
    app.step(:pre_es_transform) {}
    app.step(:es_load) {}
    
    # Initially the run is run. It fails.
    first_run_time = Time.now
    Timecop.freeze(first_run_time) do
      app.run
      app.failed?.should == true

      app.last_attempt.should == Time.now.to_i
      app.last_successful_start.should == nil
      app.last_successful_finish.should == nil
      app.last_full_run_start.should == nil
      app.current_full_run_start.should == first_run_time.to_i
    end

    # sometimes later the PD notices it is wrong and it restarts the project from last checkpoint. It fails again.
    second_run_time = Time.now
    Timecop.freeze(second_run_time) do
      app.restart_from_last_checkpoint
      app.failed?.should == true
      app.last_attempt.should == second_run_time.to_i
      app.last_successful_start.should == nil
      app.last_successful_finish.should == nil
      app.last_full_run_start.should == nil
      app.current_full_run_start.should == first_run_time.to_i
    end

    # Again it starts it again. Now it succeeds
    introduce_bug = false
    third_run_time = Time.now
    Timecop.freeze(third_run_time) do
      app.restart_from_last_checkpoint
      app.failed?.should == false
      app.last_attempt.should == third_run_time.to_i
      app.last_successful_start.should == third_run_time.to_i
      app.last_successful_finish.should == third_run_time.to_i
      app.last_full_run_start.should == first_run_time.to_i
      app.current_full_run_start.should == first_run_time.to_i
    end

    # It is the other day and it runs again
    the_other_day_time = Time.now
    Timecop.freeze(the_other_day_time) do
      app.run
      app.failed?.should == false
      
      app.last_attempt.should == the_other_day_time.to_i
      app.last_successful_start.should == the_other_day_time.to_i
      app.last_successful_finish.should == the_other_day_time.to_i
      app.last_full_run_start.should == the_other_day_time.to_i
      app.current_full_run_start.should == the_other_day_time.to_i
    end

  end

  it "should NOT execute after_failure blocks after it runs" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}

    mail_to_pd = double("PD mailer")
    mail_to_pd.stub(:call).and_return(true)
    mail_to_pd.should_not_receive(:call)
    app.after_failure(mail_to_pd)
    app.run
  end

  it "should execute after_failure blocks after it runs and fails" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {fail}

    mail_to_pd = double("PD mailer")
    mail_to_pd.stub(:call).and_return(true)
    mail_to_pd.should_receive(:call)
    app.after_failure(mail_to_pd)
    app.run
  end

  it "should execute after_failure blocks after it runs and exits" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {exit}

    mail_to_pd = double("PD mailer")
    mail_to_pd.stub(:call).and_return(true)
    mail_to_pd.should_not_receive(:call)
    app.after_failure(mail_to_pd)
    app.run
  end

  it "should execute after_success blocks after it runs" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}

    mail_to_pd = double("PD mailer")
    mail_to_pd.stub(:call).and_return(true)
    mail_to_pd.should_receive(:call)
    app.after_success(mail_to_pd)
    app.run
  end

  it "should NOT execute after_failure blocks after it runs and fails" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {fail}

    mail_to_pd = double("PD mailer")
    mail_to_pd.stub(:call).and_return(true)
    mail_to_pd.should_not_receive(:call)
    app.after_success(mail_to_pd)
    app.run
  end

  it "should execute after_failure blocks after it runs and exits" do
    app = Infra::App.new(:logger => Logger.new(File.open('/dev/null', 'a')))
    app.sequence.each {|name| app.step(name) {}}
    app.step(:download) {exit}

    mail_to_pd = double("PD mailer")
    mail_to_pd.stub(:call).and_return(true)
    mail_to_pd.should_receive(:call)
    app.after_success(mail_to_pd)
    app.run
  end

end
