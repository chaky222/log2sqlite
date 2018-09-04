class Log2SQLite::LogFilesRow < Log2SQLite::BaseRecord
  @@table       = "log_files_rows"
  @@table_alias = "log_files_rows"


  def self.split_line_to_arr(line : String) : Array(String)
    result = [] of String
    cur_val : Array(Char) = Array.new(6400, ' ')
    # cur_val = String.new(6400) { |x| { 6400, 0 } }
    bracket_closed = true
    quotes_closed = true
    index = 0
    line.each_char do |c|
      bracket_closed = false if quotes_closed && c == '['
      bracket_closed = true  if quotes_closed && c == ']'
      quotes_closed  = !quotes_closed if c == '"'
      if (c == ' ' || c == ']') && bracket_closed && quotes_closed
        if index > 0
          result.push(cur_val[0..index - 1].join)
          index = 0
        end
      else
        cur_val[index] = c
        index += 1
      end
    end
    # puts "\n\n result=[\n#{ result.join("\n") }\n] \n\n"
    result
  end

  def self.data_from_str(line : String) : Hash(Symbol, String)
    raw_data = {} of Symbol => String
    line_arr : Array(String) = split_line_to_arr(line)
    request_strs = (line_arr[4]? || "NO_DATA_1 NO_DATA_2 NO_DATA_3 NO_DATA_4").split(' ')
    if line.size > 10 && line_arr.size > 7 && request_strs.size > 1
      json_ends : Array(String) = (line_arr.size > 1 ? "#{ line_arr.last }".split(',') : ["['NO_JSON_DATA', 'NO_JSON_DATA2', 'NO_JSON_DATA3']"]).map { |x| x.strip }
      # puts "\n\n index=[#{ index }] json_ends=[#{ json_ends }] \n\n"
      raw_data[:user_name] = line_arr[2]? || "NO_DATA"

      raw_data[:method] = "#{ request_strs.first }".lchop
      raw_data[:url] = "#{ request_strs[1] }"
      raw_data[:referer] = (line_arr[7]? || "'NO_DATA_7_1'").lchop.rchop
      raw_data[:user_borwser] = (line_arr[8]? || "''").lchop.rchop
      raw_data[:responce_code] = ((line_arr[5]? || "-2").to_i? || -1).to_s
      raw_data[:upstream_response_time] = (("#{ json_ends[2]? || "ssss" }").lchop.rchop.to_f? || -1_f32).to_s
      raw_data[:sid] = (json_ends[0].not_nil!).lchop.lchop.rchop
      raw_data[:post_form] = (json_ends[1]? || "''").lchop.rchop
      request_time : String = (line_arr[3]? || "[]").lchop
      # raw_data[:request_time] = request_time.size > 5 ? ::Time.parse(request_time, "%F %H:%M:%S.%N", location: Time::Location.local) : Time.epoch(0)
      raw_data[:request_time] = (request_time.size == "01/Sep/2018:12:54:19 +0300".size ? ::Time.parse(request_time, "%d/%b/%Y:%T %z", location: Time::Location.local) : Time.epoch(0)).to_s("%F %T")

    else
      raw_data[:invalid] = "1"
    end
    raw_data
  end

  def self.new_from_string(sqlite3_db_in : DB::Database, log_files_id : Int32, offset : Int32, str : String) : self | Nil
    data = data_from_str(str)
    return nil if data[:invalid]?
    result = new(sqlite3_db_in, 0).set_attributes(data.merge({ :log_files_id => log_files_id.to_s, :offset => offset.to_s }))
    # result.save!
    result
  end

  def self.save_all_new_items_UNSAFE(sqlite3_db_in : DB::Database, new_items = Array(self)) : Bool
    fields   = [] of String
    new_vals_buffer = [] of String
    record_vals.each_with_index do |k, v, i|
      unless [:id, :updated_at, :created_at].includes?(k)
        fields << '`' + DB.quote(k.to_s, true) + '`'
      end
    end
    new_items.each do |item|
      raise("ERROR 545646. save_all_new_items_UNSAFE working ONLY for new items. Sorr.") if item.id > 0
      new_vals = [] of String
      record_vals.each_with_index do |k, v, i|
        unless [:id, :updated_at, :created_at].includes?(k)
          new_vals << "#{ item.@raw_data[i].is_a?(Time) ? DB.quote(item.@raw_data[i].as(Time).to_s("%F %T")) : DB.quote(item.@raw_data[i]) }"
        end
      end
      new_vals << "datetime('now')" # updated_at
      new_vals << "datetime('now')" # created_at
      new_vals_buffer << "(#{ new_vals.join(',') })"
    end

    set_vals : String = new_vals_buffer.join(',')
    ins = "INSERT INTO #{ @@table } ( #{ fields.join(',') }, updated_at, created_at) VALUES #{ set_vals }"

    puts "\n\n save ins=[#{ ins }] \n\n"
    res = sqlite3_db_in.exec(ins)
    result_cnt = res.rows_affected
    if result_cnt != new_items.size
      puts "\n\n\n ERROR 324434234! rows_affected=[#{ result_cnt }] new_items.size=[#{ new_items.size }] \n\n\n"
    end
    result_cnt == new_items.size
  end

  def self.record_vals
    { id: Int32, log_files_id: Int32, offset: Int32, updated_at: Time, created_at: Time,
      request_time: Time, user_name: String, sid: String, url: String, method: String, responce_code: Int32, referer: String, user_borwser: String,
      post_form: String, upstream_response_time: Float32 }
  end

  def self.migrate(sqlite3_db_in : DB::Database) : Bool
    super(sqlite3_db_in)
    sqlite3_db_in.exec("CREATE TABLE IF NOT EXISTS log_files_rows(id INTEGER NOT NULL PRIMARY KEY, log_files_id INTEGER, offset INTEGER, updated_at DATETIME, created_at DATETIME,
      request_time DATETIME, user_name VARCHAR(25), sid VARCHAR(25), url TEXT, method VARCHAR(5), responce_code INTEGER, referer TEXT, user_borwser TEXT, post_form TEXT, upstream_response_time REAL)")
    sqlite3_db_in.exec("CREATE INDEX IF NOT EXISTS log_files_rows_log_files_id ON log_files_rows(log_files_id)")
    sqlite3_db_in.exec("CREATE INDEX IF NOT EXISTS log_files_rows_offset ON log_files_rows(offset)")

    true
  end

end