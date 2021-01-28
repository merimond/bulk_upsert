module BulkUpsert
  module Query

    DEFAULT_OPTIONS = {
      merge_table:      "merged",
      merge_key:        "_found_id",
      allow_belongs_to: false,
      ignore_conflicts: false,
      allow_nulls:      false,
      skip_find:        false,
    }

    def self.merge_query(models, model:, merge_key:, **options)
      models = models.uniq { |m| m.search_atts_as_json }
      search_list = models.map(&:search_atts_as_json)
      update_list = models.map(&:as_json)

      search_json = ActiveRecord::Base.connection.quote(search_list.to_json)
      update_json = ActiveRecord::Base.connection.quote(update_list.to_json)

      opts = options.merge(input_table: "input", model: model)
      join = format_join_conditions(search_list, **opts)

      <<-eos
        SELECT
          (jsonb_populate_record(NULL::#{model.table_name}, atts_to_update)).*,
          #{model.table_name}.#{model.primary_key} AS #{merge_key}
        FROM (
          SELECT
            (jsonb_populate_recordset(NULL::#{model.table_name}, #{search_json})).*,
            (jsonb_array_elements(#{update_json})) AS atts_to_update
        ) #{opts[:input_table]}
        LEFT JOIN
          #{model.table_name}
        ON
          #{join.join(" AND ")}
      eos
    end

    def self.insert_query(models, model:, to_search:, to_update:, merge_table:, merge_key:, ignore_conflicts: false, **options)
      to_insert = (to_search | to_update) - [model.primary_key]
      to_return = (to_insert | [model.primary_key]).sort

      <<-eos
        INSERT INTO
          #{model.table_name} (#{to_insert.join(", ")})
        SELECT
          #{to_insert.join(", ")}
        FROM
          #{merge_table}
        WHERE
          #{merge_table}.#{merge_key} IS NULL
          #{ignore_conflicts ? "ON CONFLICT DO NOTHING" : ""}
        RETURNING
          #{to_return.join(", ")}
      eos
    end

    def self.update_query(models, model:, to_search:, to_update:, merge_table:, merge_key:, **options)
      cols = (to_search | to_update | [model.primary_key]).sort
      cols = cols.map { |a| "result.%s" % a }

      opts = { source_table: merge_table, result_table: "result" }
      atts = format_update_atts(models, **opts)

      <<-eos
        UPDATE
          #{model.table_name} AS #{opts[:result_table]}
        SET
          #{atts.empty? ? "#{model.primary_key} = #{opts[:result_table]}.#{model.primary_key}" : atts.join(", ")}
        FROM
          #{merge_table}
        WHERE
          #{opts[:result_table]}.#{model.primary_key} = #{merge_table}.#{merge_key}
        RETURNING
          #{cols.join(", ")}
      eos
    end

    def self.straight_insert_query(models, model:, to_search:, to_update:, **options)
      to_insert = (to_search | to_update) - [model.primary_key]
      update_list = models.map(&:as_json)
      update_json = ActiveRecord::Base.connection.quote(update_list.to_json)

      <<-eos
        INSERT INTO
          #{model.table_name} (#{to_insert.join(", ")})
        SELECT
          #{to_insert.join(", ")}
        FROM
          jsonb_populate_recordset(NULL::#{model.table_name}, #{update_json})
        RETURNING
          id
      eos
    end

    def self.to_sql(models, allow_belongs_to: false, skip_find: false, **options)
      klasses = models.map(&:klass).uniq
      model   = klasses.first

      if klasses.count > 1
        raise MultipleClassesError.new(klasses.map(&:name))
      end

      to_search = models.map(&:columns_to_search).flatten.uniq
      to_update = models.map(&:columns_to_update).flatten.uniq

      if to_update.include?(model.primary_key)
        raise PrimaryKeyUpdateError.new(model.primary_key)
      end

      opts = DEFAULT_OPTIONS.merge(options).merge({
        model:       model,
        to_search:   to_search,
        to_update:   to_update,
      })

      unless allow_belongs_to
        model.reflections.each do |key, ref|
          if ref.through_reflection?
            next
          end
          unless col = ref.foreign_key
            next
          end
          if ref.options[:optional]
            next
          end
          if to_search.include?(col) || to_update.include?(col)
            raise BelongsToDeficiencyError.new(col, model)
          end
        end
      end

      if skip_find
        return straight_insert_query(models, **opts)
      end

      if to_search.empty?
        raise EmptySearchListError.new
      end

      <<-eos
        WITH #{opts[:merge_table]} AS (
          #{merge_query(models, **opts)}
        ), updated AS (
          #{update_query(models, **opts)}
        ), inserted AS (
          #{insert_query(models, **opts)}
        )
        SELECT * FROM updated
        UNION
        SELECT * FROM inserted
      eos
    end

    def self.assign_ids(models, result, skip_find: false, **options)
      if skip_find
        if result.count != models.count
          raise ResultCountMismatch.new(result, models)
        end
        return models.each_with_index do |model, idx|
          model.id = result[idx]["id"]
        end
      end

      models.each do |model|
        result.each do |row|
          model.assign_id_from_hash(row)
        end
      end
    end

    def self.execute(models, connection: nil, skip_id_assignment: false, **options)
      valid = models.reject(&:valid?)
      return [] if valid.empty?

      query = to_sql(valid, **options)
      connection ||= ActiveRecord::Base.connection
      result = connection.execute(query).to_a
      assign_ids(valid, result, **options) unless skip_id_assignment

      valid
    end

    def self.format_join_conditions(list, model:, to_search:, input_table:, allow_nulls: false, **options)
      cols_with_nils = to_search.select do |col|
        list.any? { |hash| hash[col].nil? }
      end

      unless allow_nulls
        unless list.map(&:keys).map(&:sort).uniq.count == 1
          raise InconsitentAttributeError.new
        end
        unless cols_with_nils.empty?
          raise MissingValueError.new(model, cols_with_nils)
        end
      end

      to_search.map do |col|
        has_nils = allow_nulls && cols_with_nils.include?(col)
        equality = has_nils ? "IS NOT DISTINCT FROM": "="
        "#{model.table_name}.#{col} #{equality} #{input_table}.#{col}"
      end
    end

    def self.format_update_atts(models, source_table:, result_table:)
      flags = models.reduce({}) do |result, model|
        model.atts_to_update.each do |att|
          current = result[att.name] ||= att.flag
          next if current == att.flag
          raise InconsitentFlagError.new(att.name, [current, att.flag])
        end
        result
      end

      flags.map do |name, flag|
        case flag
        when :maybe
          # Update record only if existing value is nil
          "#{name} = COALESCE(#{result_table}.#{name}, #{source_table}.#{name})"
        when :prefer
          # Update record only if the new value is not nil
          "#{name} = COALESCE(#{source_table}.#{name}, #{result_table}.#{name})"
        when :always
          # Updates record in any case
          "#{name} = #{source_table}.#{name}"
        else
          raise InvalidFlagError.new(name, flag)
        end
      end
    end

  end
end
