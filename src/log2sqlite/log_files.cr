require "sqlite3"


class Log2SQLite::LogFiles < Log2SQLite::BaseRecord
  @@table       = "log_files"
  @@table_alias = "log_files"

  # def initialize(sqlite3_db_in : DB::Database, params : Hash(String, String))
  #   super(sqlite3_db_in, params)
  # end
  def file_size         : Int32;  val_by_field(:file_size          ).as(Int32?) || 0; end
  def crc32_first_symbs : UInt32; ((val_by_field(:crc32_first_symbs).as(String?) || "0").to_i64? || 0).to_u32; end

  # def offset_done       : Int32; val_by_field(:offset_done       ).as(Int32?) || 0; end


  def load_more_rows_from_file(file_name : String) : self
    tmp_file_name = file_name + ""

    if tmp_file_name.ends_with?(".gz")
      filename_short = tmp_file_name.split('/').last? || "ERR_NO_NAME"
      filename_short = "ERR223_NO_NAME" unless filename_short.size > 5
      new_file_name = "/tmp/unpacked_load_more_rows_from_file_#{ id }_#{ filename_short }"
      File.delete(new_file_name) if File.file?(new_file_name)
      cmd = "uncompress -c #{ tmp_file_name } > #{ new_file_name }"
      %x{#{ cmd }}
      # puts "\n\n\n try cmd=[#{ cmd }] check new_file_name=[#{ new_file_name }] \n\n\n"
      tmp_file_name = new_file_name
    end
    save! unless id > 0
    f = File.open(tmp_file_name)


    puts "\n\n load_more_rows_from_file tmp_file_name=[#{ tmp_file_name }] \n\n"
    offset = offset_done
    size = f.size.to_i
    rows_for_save : Array(Log2SQLite::LogFilesRow) = [] of Log2SQLite::LogFilesRow
    (0..100_000_000).each do |i|
      f.read_at(offset, Math.min(100_000, size - offset)) do |buf|
        s : String = buf.gets() || "NO_DATA"
        offset = offset + s.size + 1
        new_row = Log2SQLite::LogFilesRow.new_from_string(@sqlite3_db, id, offset, s)
        rows_for_save << new_row.not_nil! if new_row
        puts "\n s=[#{ s }] \n"
      end
      break if offset >= size
    end
    File.delete(tmp_file_name) if tmp_file_name.starts_with?("/tmp/")
    Log2SQLite::LogFilesRow.save_all_new_items_UNSAFE(@sqlite3_db, rows_for_save)
    # set_attributes({ "offset_done" => offset.to_s }).save!
    puts "\n\n done load_more_rows_from_file tmp_file_name=[#{ tmp_file_name }]  \n\n"
    save!
    self
  end

  def offset_done : Int32
    result = @sqlite3_db.sel_i32!("SELECT MAX(#{ Log2SQLite::LogFilesRow.record_fields([:offset]) }) as max_offset FROM #{ Log2SQLite::LogFilesRow.aa_table } WHERE log_files_id=#{ id }")
    puts "\n\n offset_done=#{ result } \n\n\n"
    result
  end

  def self.record_vals
    { id: Int32, name: String, base_file_name: String, file_index: Int32, file_size: Int32, crc32: String, crc32_first_symbs: String, modify_time: Time, updated_at: Time, created_at: Time }
  end

  def destroy! : Hash(String, Array(Int32))
    Log2SQLite::LogFilesRow.delete_from_db_all(@sqlite3_db, { :log_files_id => { eq: id } })
    super()
  end

  def self.migrate(sqlite3_db_in : DB::Database) : Bool
    super(sqlite3_db_in)
    sqlite3_db_in.exec("CREATE TABLE IF NOT EXISTS log_files(id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(155), base_file_name VARCHAR(150),
                          file_index INTEGER, file_size INTEGER, crc32 VARCHAR(155), crc32_first_symbs VARCHAR(15), modify_time DATETIME,
                          updated_at DATETIME, created_at DATETIME)")
    sqlite3_db_in.exec("CREATE INDEX IF NOT EXISTS log_files_name  ON log_files(name)")
    sqlite3_db_in.exec("CREATE INDEX IF NOT EXISTS log_files_crc32 ON log_files(crc32)")

    true
  end

end