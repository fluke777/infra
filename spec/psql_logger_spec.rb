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
  
  it "should return nil if psql_logger was not initialized" do
    @app.psql_logger.should == nil
  end
  
  it "should return false if non-production and psql_logger is nil" do
    @app.do_psql_log?.should == false
  end
  
  it "should return false if non-production" do
    @app.psql_logger = []
    @app.do_psql_log?.should == false
  end
  
  it "should return false if psql_logger is not set" do
    @app.is_production = true
    @app.do_psql_log?.should == false
  end
  
  it "should return true if psql_logger is set and on production" do
    @app.is_production = true
    @app.psql_logger = []
    @app.do_psql_log?.should == true
  end
  
end