require "bulk_upsert/attribute"
require "bulk_upsert/exception"
require "bulk_upsert/model"
require "bulk_upsert/operation"
require "bulk_upsert/query"

module BulkUpsert
  def self.save_group(models, *args)
    Query.new(models).execute(*args)
  end

  def self.build(klass, atts_to_search, atts_to_update = {}, &block)
    unless atts_to_search.is_a?(Hash)
      raise InvalidSearchHashError.new(atts_to_search)
    end
    model = Model.new(klass, atts_to_search)
    atts_to_update.each do |key, value|
      model.always(key, value)
    end
    if block_given?
      yield model
    end
    model
  end

  def self.save(models, *args)
    if models.empty?
      return []
    end
    # TODO: add a write-up along the following lines:
    #
    # Grouping by tables makes more sense, but it causes issues
    # when models use different sets of attributes for searching
    #
    # E.g. Model A searches columns X and Y, Model B searches columns Y and Z
    # 
    # A naive JOIN (x = x AND y = y AND z = z) will fail because
    # either Y column (in case of Model B) or Z column (in case of
    # Model A) will be NULL, and NULL don't work very well with
    # equality operators.
    # 
    # If we switch to `IS NOT DISTINCT FROM`, the queries will
    # become painfully slow, as PG will stop using indices (see 1).
    #
    # [1] https://www.postgresql-archive.org/IS-NOT-DISTINCT-FROM-Indexing-td5812296.html
    #

    list  = models.group_by(&:klass).values
    group = list.find { |g| g.all?(&:ready?) } ||
            list.find { |g| g.any?(&:ready?) }

    if group.nil? && models.all?(&:valid?)
      return []
    end

    current = save_group(group, *args)
    others  = save(models - group, *args)
    current + others
  end

end
