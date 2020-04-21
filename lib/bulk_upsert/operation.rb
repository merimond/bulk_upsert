module BulkUpsert
  module Operation

    attr_reader :models

    def initialize(*args, &block)
      @models = []
      super(*args, &block)
    end

    def build(klass, atts_to_search, atts_to_update = {})
      BulkUpsert.build(klass, atts_to_search, atts_to_update) do |model|
        yield model if block_given?
        models << model
      end
    end

  end
end
