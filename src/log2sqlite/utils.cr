module DB
  class Database
    def query_all5(query, *args, as types : NamedTuple)
      puts "\n query_all5=[#{ query }] \n types=[#{ types.inspect }] \n"
      using_connection do |conn|
        conn.query_all(query, *args) do |rs|
          rs.read(**types)
        end
      end
    end

    def sel_i64(sql : String) : Int64 | Nil
      result : Int64 | Nil = nil
      query_all5(sql, as: { s: Int64? }).each_with_index do |row, index|
        raise("ERROR 65445645. Too many records in sel_i64! sql=[#{ sql }]") if index > 0
        result = row[:s]
      end
      result
    end

    def sel_i64!(sql : String) : Int64
      sel_i64(sql) || 0_i64
    end

    def sel_i32(sql : String) : Int32?
      result : Int32 | Nil = nil
      query_all5(sql, as: { s: Int32? }).each_with_index do |row, index|
        raise("ERROR 654456451. Too many records in sel_i32! sql=[#{ sql }]") if index > 0
        result = row[:s]
      end
      result
    end

    def sel_i32!(sql : String) : Int32
      sel_i32(sql) || 0
    end
  end
  # contacts = db.query_all "select name, age from contacts", as: {name: String, age: Int32}
  def self.quote(in_val : DB::Any, strip_quotes : Bool = false) : String
    # result : String = PG::EscapeHelper.escape_literal(s)
    s : String = in_val.is_a?(Time) ? in_val.as(Time).to_s("%F %T") : "#{ in_val }"
    result : String = s.gsub('\'', "\\\'").gsub('"', "\\\"").gsub('`', "\\\`")
    strip_quotes ? result : ('\'' + result + '\'')
  end

  def self.quote_like(in_val : DB::Any) : String
    quote(in_val, true).gsub('_', "\\_")
  end

  def self.q_time(time_in : Time) : String
    "#{ quote(time_in.to_s("%F %T")) }"
  end

  # def self.sel_i64!(db : DB::Database, sql : String) : Int64
  #   sel_i64(db, sql) || 0_i64
  # end

  # def self.sel_i64(db : DB::Database, sql : String) : Int64 | Nil
  #   result : Int64 | Nil = nil
  #   db.query_all5(sql, as: { s: Int64 }).each_with_index do |row, index|
  #     raise("Too many records in sel_i32! sql=[#{ sql }]") if index > 0
  #     result = row[:s]
  #   end
  #   result
  # end

  # def self.sel_i32!(db : DB::Database, sql : String) : Int32
  #   sel_i32(db, sql) || 0
  # end

  # def self.sel_i32(db : DB::Database, sql : String) : Int32 | Nil
  #   result : Int32 | Nil = nil
  #   arr = sel_i32s(db, sql)
  #   raise("Too many records in sel_i32! sql=[#{ sql }]") if arr.size > 1
  #   result = arr.first if arr.size == 1
  #   result
  # end

  # def self.sel_i32s(db : DB::Database, sql : String) : Array(Int32)
  #   result : Array(Int32) = [] of Int32
  #   db.query_all5(sql, as: { s: Int32 }).each_with_index do |row, index|
  #     result << row[:s]
  #   end
  #   result
  # end

  # def self.sel_time(db : DB::Database, sql : String) : Time | Nil
  #   result : Time | Nil = nil
  #   db.query_all5(sql, as: { s: Time }).each_with_index do |row, index|
  #     raise("Too many records in sel_i32! sql=[#{ sql }]") if index > 0
  #     # result = row[:s] - 3.hour
  #     result = row[:s]
  #   end
  #   result
  # end

  # def self.sel_str(db : DB::Database, sql : String) : String | Nil
  #   result : String | Nil = nil
  #   db.query_all5(sql, as: { s: String }).each_with_index do |row, index|
  #     raise("Too many records in sel_str! sql=[#{ sql }]") if index > 0
  #     result = row[:s]
  #   end
  #   result
  # end

  # def self.pages_sql(raw : Hash(String, Array(String)), ppp : Int32 = 50) : String
  #   current_page : Int32 = raw["page"]? ? "#{ raw["page"].first? }".to_i? || 0 : 0
  #   current_page = 1 unless current_page > 0
  #   " LIMIT #{ (current_page - 1) * ppp }, #{ ppp } "
  # end

end

