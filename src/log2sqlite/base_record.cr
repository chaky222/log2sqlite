require "sqlite3"

class Log2SQLite::BaseRecord
  @@table : String = ""
  @@table_alias : String = "t"

  @sqlite3_db : DB::Database
  @raw_data : Array(DB::Any)
  @db_raw_data : Array(DB::Any) | Nil = nil
  @in_edit_mode : Bool = false
  @errors : Log2SQLite::PErrors = Log2SQLite::PErrors.new()


  def id   : Int32 ; val_by_field(:id  ).as(Int32  );       end
  def name : String; val_by_field(:name).as(String?) || ""; end

  def initialize(sqlite3_db_in : DB::Database, new_id : Int32)
    raise("ERROR 2343432432. Can not create a new example of a model[#{ @@table }].") if new_id > 0
    @sqlite3_db = sqlite3_db_in
    @raw_data = [] of DB::Any
    self.class.record_vals.each_with_index do  |k_sym, v_class, index|
      val : DB::Any | Nil = nil
      val = new_id if k_sym == :id
      @raw_data << val
    end
  end

  def initialize(sqlite3_db_in : DB::Database, raw_data_in : Array(DB::Any))
    @sqlite3_db = sqlite3_db_in
    @raw_data = raw_data_in
  end

  def val_by_field(field_name : Symbol) : DB::Any
    self.class.record_vals.each_with_index do  |k_sym, v_class, index|
      return @raw_data[index] if k_sym == field_name
    end
    raise("ERROR 453453. Not found field [#{ field_name }] for table=[#{ @@table }]")
    0.as(DB::Any)
  end

  def self.record_vals
    { id: Int32, name: String }
  end

  def self.record_fields(selected_fields : Array(Symbol) | Nil = nil) : String
    flds : Array(String) = [] of String
    record_vals.each_with_index do |k, v_class, i|
      next if selected_fields && (!(selected_fields.includes?(k)))
      str : String =  "#{ @@table_alias }." + '`' + DB.quote("#{ k }", true) + '`'
      # flds.push(v_class.is_a?(Time.class) ? "strftime('%s', #{ str }) as #{ k }_unix_stamp" : str)
      flds.push(str)
    end
    flds.join(", ")
  end

  def self.field_present?(field_name : Symbol | String) : Bool
    record_vals.each_with_index do  |k_sym, v_class, index|
      return true if k_sym.to_s == field_name.to_s
    end
    false
  end

  def self.record_vals_links
    { log_file_id: Log2SQLite::LogParser }
  end

  def self.scope_criterias(attrs : Hash(Symbol | String, NamedTuple(eq: DB::Any) | NamedTuple(less: DB::Any) | NamedTuple(more: DB::Any))) : String
    where_arr = [] of String
    attrs.each do |key, val_tupl|
      raise("ERROR 43242342. No key=[#{ key }] in model=[#{ self.name }]") unless field_present?(key)
      val : String = ""
      if val_tupl[:eq]?
        val = "= #{ DB.quote("#{ val_tupl.as(NamedTuple(eq: DB::Any))[:eq] }") }"
      elsif val_tupl[:less]?
        val = "< #{ DB.quote("#{ val_tupl.as(NamedTuple(less: DB::Any))[:less] }") }"
      elsif val_tupl[:more]?
        val = "> #{ DB.quote("#{ val_tupl.as(NamedTuple(more: DB::Any))[:more] }") }"
      end
      where_arr << "(#{ aa_table }.#{ DB.quote("#{ key }", true) } #{ val })"
    end
    where_arr.join(" AND ")
  end

  def self.by_criterias(sqlite3_db_in : DB::Database, attrs : Hash(Symbol | String, NamedTuple(eq: DB::Any) | NamedTuple(less: DB::Any) | NamedTuple(more: DB::Any))) : Array(self)
    all(sqlite3_db_in, " WHERE #{ scope_criterias(attrs) }")
  end

  def self.all(sqlite3_db_in : DB::Database, where_clause : String = "") : Array(self)
    result = [] of self
    sqlite3_db_in.query_all5("SELECT #{ record_fields } FROM #{ table } #{ where_clause }", as: record_vals).each do |row|
      result << new(sqlite3_db_in, row.map { |k_sym, value| value.as(DB::Any) })
    end
    result
  end

  def self.by_id(sqlite3_db_in : DB::Database, id : Int32 | Int64, use_cache : Bool = true) : self | Nil
    by_criterias(sqlite3_db_in, { :id => { eq: id.as(DB::Any) } }).first?
  end

  def set_attributes(attrs_in : Hash(Symbol | String, String | Nil), update_only : Bool = false) : self
    @db_raw_data = @raw_data.clone if (!(@db_raw_data))
    @in_edit_mode = true
    # puts "\n\n\n set_attributes attrs=[#{ attrs.inspect }] \n\n\n\n"
    attrs = {} of String => String | Nil
    attrs_in.each { |k, v| attrs["#{ k }"] = v }
    # new_raw_data : Array(DB::Any) = Array.new(self.class.record_vals.keys.size) { |i| nil.as(DB::Any) }
    new_raw_data : Array(DB::Any) = [] of DB::Any
    self.class.record_vals.each_with_index do |key, v_class, index|
      if attrs.has_key?("#{ key }")
        if attrs["#{ key }"] # if not nil
          str = "#{ attrs["#{ key }"] }"
          val : DB::Any = nil
          if v_class.is_a?(Int32.class)
            val = str.to_i
          elsif v_class.is_a?(Float32.class)
            val = str.to_f32
          elsif v_class.is_a?(Time.class)
            val = Time.parse(str, "%F %H:%M:%S", location: Time::Location::UTC)
          else
            val = "#{ str }".strip
          end
          new_raw_data << val
        else
          new_raw_data << attrs["#{ key }"]
        end
      else
        new_raw_data << @raw_data[index]
      end
    end
    @raw_data = new_raw_data
    self
  end

  def validate! : Bool
    @errors = Log2SQLite::PErrors.new()
    @errors.empty?
  end

  def valid? : Bool
    validate! && @errors.empty?
  end

  def save! : Bool
    if valid?
      # self_h : Hash(Symbol, DB::Any) = to_h

      fields   = [] of String
      new_vals = [] of String
      self.class.record_vals.each_with_index do |k, v, i|
        unless [:id, :updated_at, :created_at].includes?(k)
          fname : String = '`' + DB.quote(k.to_s, true) + '`'
          fields << fname
          new_vals << (id > 0 ? fname + '=' : "") + "#{ @raw_data[i].is_a?(Time) ? DB.quote(@raw_data[i].as(Time).to_s("%F %T")) : DB.quote(@raw_data[i]) }"
        end
      end
      set_vals : String = new_vals.join(", ")
      where_add : String = " (1) "
      ins : String = ""
      if id > 0
        ins = "UPDATE #{ @@table } SET #{ set_vals }, updated_at=datetime('now') WHERE #{ where_add } AND (id=#{ id })"
      else
        ins = "INSERT INTO #{ @@table } ( #{ fields.join(',') }, updated_at, created_at) VALUES (#{ set_vals }, datetime('now'), datetime('now'))"
      end
      # = "#{ id > 0 ? "UPDATE" : "INSERT INTO" }  #{ id > 0 ? "" : ", created_at=NOW()" }"
      puts "\n\n save ins=[#{ ins }] \n\n"
      res = @sqlite3_db.exec(ins)
      if res.rows_affected > 0
        tmp_id : Int32 = id > 0 ? id : res.last_insert_id.to_i
        set_attributes({ "id" => tmp_id.to_s })

        # and refill data from DB!
        new_item = self.class.by_id(@sqlite3_db, id, false)
        raise("Error 567567521. Not found saved item #{ id } in table [#{ @@table }]") unless new_item
        @raw_data = new_item.not_nil!.@raw_data
        @db_raw_data = @raw_data.clone
        @in_edit_mode = false
      else
        @errors.push("save", "Не удалось сохранить в #{ @@table }.")
      end
    else
      @errors.push("save", "Не удалось сохранить. Валидация не пройдена.")
    end
    @errors.empty?
  end

  def destroy! : Hash(String, Array(Int32))
    result = {} of String => Array(Int32)
    if delete_from_db!
      result[self.class.aa_table] = [id]
    end
    result
  end

  def delete_from_db! : Bool
    result = false
    if id > 0
      result = self.class.delete_from_db_all(@sqlite3_db, { :id => { eq: id } }) > 0
    end
    result
  end

  def self.delete_from_db_all(sqlite3_db_in : DB::Database, attrs : Hash(Symbol | String, NamedTuple(eq: DB::Any) | NamedTuple(less: DB::Any) | NamedTuple(more: DB::Any))) : Int64
    sql = "DELETE FROM #{ @@table } WHERE #{ scope_criterias(attrs) }"
    puts "\n\n delete_from_db_all sql=[#{ sql }] \n\n\n"
    res = sqlite3_db_in.exec(sql)
    res.rows_affected
  end



  def self.table : String
    "#{ @@table } as #{ @@table_alias }"
  end

  def self.aa_table : String
    @@table
  end

  def self.migrate(sqlite3_db_in : DB::Database) : Bool
    true
  end
end