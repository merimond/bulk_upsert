module BulkUpsert

  class Error < StandardError; end

  class ValidModelsMissingError < Error
    def initialize(klasses)
      super "Failed to find valid models for #{klasses.join(", ")}"
    end
  end

  class PendingInvalidModelsError < Error
    def initialize(klasses, messages = [])
      super "Validation errors in #{klasses.join(", ")}: #{messages.join(", ")}"
    end
  end

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

  class InconsitentAttributeError < Error
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
    def initialize(klass, columns)
      super "#{klass.name} has NULL values in #{columns.inspect}. Consider setting `allow_nulls` flag"
    end
  end

  class ResultCountMismatch < Error
    def initialize(result, models)
      super "Expected #{model.count} new rows for #{models.first.klass.name}, got #{result.count} instead"
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
    def initialize(column, klass)
      super("Optional flag is not set for #{klass.name}.#{column} belongs_to association")
    end
  end

end
