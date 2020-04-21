module BulkUpsert
  class Query
    attr_reader :klass, :models, :table

    def initialize(models)
      klasses = models.map(&:klass).uniq

      if klasses.count > 1
        raise MultipleClassesError.new(klasses.map(&:name))
      end

      @klass  = klasses.first
      @table  = @klass.table_name
      @pkey   = @klass.primary_key
      @models = models
    end

    def merge_query(models, merge_key: "_found_id", **options)
      models = models.uniq { |m| m.search_atts_as_json }
      search_list = models.map(&:search_atts_as_json)
      update_list = models.map(&:as_json)

      search_json = ActiveRecord::Base.connection.quote(search_list.to_json)
      update_json = ActiveRecord::Base.connection.quote(update_list.to_json)

      join = formatted_join_conditions(search_list, options.merge(
        input_table: "input",
      ))

      <<-eos
        SELECT
          (jsonb_populate_record(NULL::#{table}, atts_to_update)).*,
          #{table}.#{@pkey} AS #{merge_key}
        FROM (
          SELECT
            (jsonb_populate_recordset(NULL::#{table}, #{search_json})).*,
            (jsonb_array_elements(#{update_json})) AS atts_to_update
        ) input
        LEFT JOIN
          #{table}
        ON
          #{join.join(" AND ")}
      eos
    end

    def insert_query(models, to_search:, to_update:, merge_table: "merged", merge_key: "_found_id", **options)
      to_insert = (to_search | to_update) - [@pkey]
      to_return = to_insert | [@pkey]

      <<-eos
        INSERT INTO
          #{table} (#{to_insert.join(", ")})
        SELECT
          #{to_insert.join(", ")}
        FROM
          #{merge_table}
        WHERE
          #{merge_table}.#{merge_key} IS NULL
        RETURNING
          #{to_return.join(", ")}
      eos
    end

    def update_query(models, to_search:, to_update:, merge_table: "merged", merge_key: "_found_id", **options)
      cols = to_search | to_update | [@pkey]
      cols = cols.map { |a| "result.%s" % a }
      atts = formatted_update_atts(models, {
        source_table: merge_table,
        result_table: "result"
      })

      <<-eos
        UPDATE
          #{table} AS result
        SET
          #{atts.empty? ? "#{@pkey} = result.#{@pkey}" : atts.join(", ")}
        FROM
          #{merge_table}
        WHERE
          result.#{@pkey} = #{merge_table}.#{merge_key}
        RETURNING
          #{cols.join(", ")}
      eos
    end

    def to_sql(models, options = {})
      to_search = models.map(&:columns_to_search).flatten.uniq
      to_update = models.map(&:columns_to_update).flatten.uniq

      if to_update.include?(@pkey)
        raise PrimaryKeyUpdateError.new(@pkey)
      end

      if to_search.empty?
        raise EmptySearchListError.new
      end

      unless options[:allow_belongs_to]
        klass.reflections.select do |key, ref|
          unless col = ref.foreign_key
            next
          end
          if to_search.include?(col) || to_update.include?(col)
            raise BelongsToDeficiencyError.new(col)
          end
        end
      end
  
      options.merge!({
        to_search:   to_search,
        to_update:   to_update,
        merge_table: "merged"
      })

      <<-eos
        WITH #{options[:merge_table]} AS (
          #{merge_query(models, options)}
        ), updated AS (
          #{update_query(models, options)}
        ), inserted AS (
          #{insert_query(models, options)}
        )
        SELECT * FROM updated
        UNION
        SELECT * FROM inserted
      eos
    end

    def execute(options = {})
      valid = models.reject(&:valid?)
      return [] if valid.empty?

      query  = to_sql(valid, options)
      result = ActiveRecord::Base.connection.execute(query)
      return valid if options[:skip_id_assignment] == true

      result.to_a.each do |row|
        models.each do |model|
          model.assign_id_from_hash(row)
        end
      end

      valid
    end

    private

    def formatted_join_conditions(list, to_search:, input_table:, **options)
      if options[:skip_find]
        return ["#{table}.#{@pkey} = NULL"]
      end

      cols_with_nils = to_search.select do |col|
        list.any? { |hash| hash[col].nil? }
      end

      unless options[:allow_nulls]
        unless list.map(&:keys).map(&:sort).uniq.count == 1
          raise InconsitentAttrubuteError.new
        end
        unless cols_with_nils.empty?
          raise MissingValueError.new(cols_with_nils)
        end
      end

      to_search.map do |col|
        has_nils = options[:allow_nulls] && cols_with_nils.include?(col)
        equality = has_nils ? "IS NOT DISTINCT FROM": "="
        "#{table}.#{col} #{equality} #{input_table}.#{col}"
      end
    end

    def formatted_update_atts(models, source_table:, result_table:)
      flags = {}

      models.each do |model|
        model.atts_to_update.each do |att|
          current = flags[att.name] ||= att.flag
          next if current == att.flag
          raise InconsitentFlagError.new(att.name, [current, att.flag])
        end
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
