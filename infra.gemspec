# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "infra/version"

Gem::Specification.new do |s|
  s.name        = "infra"
  s.version     = Infra::VERSION
  s.authors     = ["Tomas Svarovsky"]
  s.email       = ["svarovsky.tomas@gmail.com"]
  s.homepage    = ""
  s.summary     = %q{MS Infratructure}
  s.description = %q{MS Infratructure}

  s.rubyforge_project = "infra"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  s.add_runtime_dependency "pry"
  s.add_runtime_dependency "gli"
  s.add_runtime_dependency "terminal-table"
  s.add_runtime_dependency "json"
  s.add_runtime_dependency "activesupport"
  s.add_runtime_dependency "actionpack"
  s.add_runtime_dependency "rainbow"
  s.add_runtime_dependency "open4"
  s.add_runtime_dependency "timecop"
  s.add_runtime_dependency "sfdc_tests"
  s.add_runtime_dependency "pony"
end