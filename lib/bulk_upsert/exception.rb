module BulkUpsert

  class Error < StandardError; end

  class MultipleClassesError < Error
    def initialize(names)
      super "Models must be instance of 1 class, got #{names.join(", ")}"
    end
  end

  class InvalidSearchHashError < Error
    def initialize(object)
      super "Hash of attributes expected, got #{object.class.name} instead"
    end
  end

  class InconsitentAttrubuteError < Error
    def initialize
      super "Different sets of attributes are used for search"
    end
  end

  class InconsitentFlagError < Error
    def initialize(name, values)
      super "`#{name}` attribute has multiple flags: #{values.inspect}"
    end
  end

  class InvalidFlagError < Error
    def initialize(name, value)
      super "`#{name}` attribute has invalid flag value: #{value.inspect}"
    end
  end

  class MissingValueError < Error
    def initialize(columns)
      super "Attribute values contain nils. Consider setting `allow_nulls` flag"
    end
  end

  class EmptySearchListError < Error
    def initialize
      super "Search attributes are empty"
    end
  end

  class PrimaryKeyUpdateError < Error
    def initialize(name)
      super "Primary key cannot be updated"
    end
  end

  class BelongsToDeficiencyError < Error
    def initialize(column)
      super("Optional flag is not set for `#{column}` belongs_to association")
    end
  end

end
