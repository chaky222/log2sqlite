require "sqlite3"
require "crc32/crc32"

class SQLite3::ResultSet < DB::ResultSet
  def read(t : Time.class) : Time
    date_format = "%F %T"
    Time.parse read(String), date_format, location: SQLite3::TIME_ZONE
  end
end

class Log2SQLite::LogParser
  @sqlite3_db : DB::Database
  @log_file_name : String

  def initialize(params : Hash(String, String))
    puts "\n\n\n params=[#{ params }] \n\n\n"
    log_file_name_tmp = params["log_file"]? || "/var/log/nginx/access.log"
    @log_file_name = File.expand_path(log_file_name_tmp)
    # log_file = File.open(@log_file_name)
    sqlite3_db_in = DB.open "sqlite3://" + (params["sqlite_db"]? || "./log2sqlite.db")
    @sqlite3_db = sqlite3_db_in
    Log2SQLite::LogFiles.migrate(sqlite3_db_in)
    Log2SQLite::LogFilesRow.migrate(sqlite3_db_in)
  end

  def run : self
    filename = @log_file_name.split('/').last || "ERR_NO_NAME"
    filename = "ERR2_NO_NAME" unless filename.size > 5
    # puts "\n\n\n run filename=[#{ filename }] log_file_name=[#{ @log_file_name }] \n\n\n"
    now_time = Time.now

    files_dir : String = @log_file_name.split('/')[0..-2].join('/')
    present_files = [] of NamedTuple(index: Int32, file_name: String)
    Dir.entries(files_dir).select { |x| x.starts_with?(filename) }.each do |fname|
      index : Int32 = fname == filename ? 0 : (fname.lchop(filename).lchop.split('.').first.to_i? || -1)
      present_files << { index: index, file_name: fname } if index >= 0
    end
    skip_remove_files : Array(Int32) = [] of Int32
    files_already_sync : Hash(String, NamedTuple(status: Int8, db_file: Log2SQLite::LogFiles)) = {} of String => NamedTuple(status: Int8, db_file: Log2SQLite::LogFiles) # 0-not syncronized, 1-partifuly syncronized, 5-done.
    present_files.sort_by { |x| x[:index] }.each do |file_tuple|
      fname = files_dir + '/' + file_tuple[:file_name]
      file_index = file_tuple[:index]
      stat = File.info(fname)
      size = stat.size
      modify_time = stat.modification_time

      new_empty = Log2SQLite::LogFiles.new(@sqlite3_db, 0).set_attributes({
        "name" => fname, "base_file_name" => @log_file_name, "file_index" => file_index.to_s, "file_size" => size.to_s,
        "modify_time" => modify_time.to_s("%F %T")
      })

      files_already_sync[fname] = { status: 0_i8, db_file: new_empty }


      old_record = Log2SQLite::LogFiles.by_criterias(@sqlite3_db, { :name => { eq: fname }, :file_size => { eq: "#{ size }" },
                                                                    :modify_time => { eq: modify_time.to_s("%F %T") },
                                                                    :updated_at => { less: now_time.to_s("%F %T") } }).first?
      if old_record
        files_already_sync[fname] = { status: 5_i8, db_file: old_record.not_nil! }
        skip_remove_files << old_record.id
      else
        crc32 = %x{crc32 #{ fname }}.strip
        crc32 = "" if crc32.includes?("\n")
        crc32_first_symbs : UInt32 = 0
        old_record = Log2SQLite::LogFiles.by_criterias(@sqlite3_db, { :base_file_name => { eq: @log_file_name }, :crc32 => { eq: crc32 }, :file_size => { eq: "#{ size }" } }).first?
        if old_record # file was ranamed by logrotate! So we must only rename it
          old_record.set_attributes({ "name" => "fname", "modify_time" => modify_time.to_s("%F %T") }).save!
          skip_remove_files << old_record.id
          files_already_sync[fname] = { status: 5_i8, db_file: old_record.not_nil! }
        else # file in first here, or maybe it is current writable file.
          max_crc_text_size = 1024 * 7
          if file_index < 2
            f = File.open(fname)
            f_size_tmp = f.size
            old_record = Log2SQLite::LogFiles.by_criterias(@sqlite3_db, { :name => { eq: fname } }).first?
            if old_record
              f_size_tmp = old_record.offset_done
            end
            f.read_at(0, Math.min(max_crc_text_size, f_size_tmp).to_i) { |buf| crc32_first_symbs = ::CRC32.checksum(buf.gets_to_end) }
            if old_record && old_record.crc32_first_symbs == crc32_first_symbs
              f.read_at(0, Math.min(max_crc_text_size, f.size).to_i) { |buf| crc32_first_symbs = ::CRC32.checksum(buf.gets_to_end) }
              old_record = old_record.not_nil!
              skip_remove_files << old_record.id
              files_already_sync[fname] = { status: 1_i8, db_file: old_record.not_nil! }
            end
            files_already_sync[fname].not_nil![:db_file].set_attributes({ "crc32_first_symbs" => crc32_first_symbs.to_s }) if crc32_first_symbs > 0
            puts "\n file[#{ file_tuple[:file_name] }] not found with crc32_first_symbs=[#{ crc32_first_symbs }] \n"
          else
            puts "\n file[#{ file_tuple[:file_name] }] in first, but index=[#{ file_index }] \n"
          end
          files_already_sync[fname].not_nil![:db_file].set_attributes({ "crc32" => crc32, "file_size" => size.to_s, "modify_time" => modify_time.to_s("%F %T") })
        end
        puts "\n\n\n fname=[#{ fname }] crc32=[#{ crc32 }] modify_time=[#{ modify_time }] \n\n\n"
      end
    end

     puts "\n Skipped files [#{ skip_remove_files }] \n"
     Log2SQLite::LogFiles.by_criterias(@sqlite3_db, { :base_file_name => { eq: @log_file_name } }).each do |log_file_in_db|
      log_file_in_db.destroy! unless skip_remove_files.includes?(log_file_in_db.id)
     end

    present_files.sort_by { |x| x[:index] }.each do |file_tuple|
      fname = files_dir + '/' + file_tuple[:file_name]
      sync_data = files_already_sync[fname].not_nil!
      db_file = sync_data[:db_file]
      if sync_data[:status] >= 5_i8
        puts "\n\n file[#{ fname }] already syncronized! \n\n"
      else
        db_file.load_more_rows_from_file(fname)
      end

    end

    # puts "\n\n files_dir=[#{files_dir}] present_files=[#{ present_files }] \n\n"



    # params = params.map {|e| e.split("=")  }

    self
  end
end