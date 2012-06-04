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
  s.add_dependency "pry"
  s.add_dependency "gooddata"
  s.add_dependency "gli"
  s.add_dependency "terminal-table"
  s.add_dependency "json"
  s.add_dependency "activesupport"
  s.add_dependency "actionpack"
  s.add_dependency "rainbow"
  s.add_dependency "open4"
  s.add_dependency "timecop"
  s.add_dependency "pony"
  s.add_dependency "downloader"
  s.add_dependency "gd"
end
