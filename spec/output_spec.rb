require 'infra'

describe Infra::App do

  it "should include set params into workspace" do
    app = Infra::App.new({
      :logger => Logger.new(File.open('/dev/null', 'a')),
    })
    ws = app.interpolate_workspace
    count = ws.find_all {|line| line.include?("foo")}.count
    count.should == 0
    
    app.set("foo", "bar")
    ws = app.interpolate_workspace
    count = ws.find_all {|line| line.include?("foo")}.count
    count.should == 1
  end

  it "should include save params into workspace" do
    app = Infra::App.new({
      :logger => Logger.new(File.open('/dev/null', 'a')),
    })
    ws = app.interpolate_workspace
    count = ws.find_all {|line| line.include?("foo")}.count
    count.should == 0
    
    app.save("bar", "baz")
    ws = app.interpolate_workspace
    count = ws.find_all {|line| line.include?("bar")}.count
    count.should == 1
  end

end


