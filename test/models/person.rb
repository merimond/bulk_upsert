class Person < ActiveRecord::Base
  def name=(value)
    self[:name] = value.is_a?(String) ? value.strip : value
  end
end
