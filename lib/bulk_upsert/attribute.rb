module BulkUpsert
  class Attribute

    attr_reader :name, :flag

    def initialize(name, value, flag = nil)
      @name  = name.to_s
      @value = value
      @flag  = flag
    end

    def ==(other)
      name == other.name && value == other.value && flag == other.flag
    end

    def search?
      flag == :search
    end

    def resolved?
      @value.instance_of?(Model) ? @value.has_id? : true
    end

    def model
      @value.is_a?(Model) ? @value : nil
    end

    def value
      @value.is_a?(Model) ? @value.id : @value
    end

  end
end
