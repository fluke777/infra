module Infra
  class Step

    attr_accessor :restartable, :block, :name, :ran, :finished

    def initialize(name, options={}, &b)
      @ran = false
      @finished = false
      @name = name.to_s
      @block = b
      @restartable = options[:restartable]
    end

    def run(app)
      app.instance_eval(&block)
    end

    def ran?
      ran
    end

    def finished?
      finished
    end

  end
end