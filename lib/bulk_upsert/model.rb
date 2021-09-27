module BulkUpsert
  class Model

    attr_accessor :id
    attr_reader :atts, :klass, :search_model, :update_model

    alias_method :model, :update_model

    def initialize(klass, atts_to_search = {})
      @id = atts_to_search[:id]
      @atts = []
      @klass = klass
      @optional = false

      @search_model = klass.new
      @update_model = klass.new

      @assoc_columns = Hash[klass.reflections
        .select { |k, v| v.belongs_to? }
        .map { |k, v| [k,v.foreign_key] }]

      atts_to_search.each do |key, value|
        add_att(key, value, :search)
      end

      if block_given?
        yield self
      end
    end

    def [](name)
      atts.select { |a| a.name == name.to_s }.map(&:value).first
    end

    def has_id?
      !id.nil?
    end

    def assign_id_from_hash(hash)
      if has_id?
        return self
      end

      match = atts_to_search.all? do |att|
        val = hash[att.name]
        # PG will not parse JSON columns automatically
        if @update_model[att.name].is_a?(Hash)
          begin
            val = JSON.parse(val)
          rescue
            val = nil
          end
        end

        val == @update_model[att.name]
      end

      if match == true
        @id = hash["id"]
      end

      self
    end

    def atts_to_search
      atts.select(&:search?)
    end

    def atts_to_update
      atts.reject(&:search?)
    end

    def columns_to_search
      atts_to_search.map(&:name).uniq
    end

    def columns_to_update
      atts_to_update.map(&:name).uniq
    end

    def valid?
      assign_to_model!; @update_model.valid?
    end

    def invalid?
      assign_to_model!; !@update_model.valid?
    end

    def ready?
      atts.all?(&:resolved?)
    end

    def search(name, value)
      add_att(key, value, :search); self
    end

    # Updates record only if existing value is nil
    def maybe(name, value)
      add_att(name, value, :maybe); self
    end

    # Updates record only if the new value is not nil
    def prefer(name, value)
      add_att(name, value, :prefer); self
    end

    # Updates record in any case
    def always(name, value)
      add_att(name, value, :always); self
    end

    def search_atts_as_json
      @search_model.attributes.slice(*columns_to_search)
    end

    def as_json
      @update_model.attributes
    end

    def mark_as_optional!
      @optional = true; self
    end

    def optional?
      @optional == true
    end

    private

    def assign_to_model!
      atts_to_search.each do |att|
        @search_model.send "#{att.name}=", att.value
      end
      atts.each do |att|
        @update_model.send "#{att.name}=", att.value
      end
    end

    def add_att(name, value, flag = nil)
      @id = value if name == "id"
      col = @assoc_columns[name.to_s] || name.to_s
      atts << Attribute.new(col, value, flag)
    end

  end
end
