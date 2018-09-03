require "db"
require "sqlite3"
require "./log2sqlite/**"

module Log2SQLite
  DATE_FORMAT = "%F %H:%M:%S.%L"
  # :nodoc:
  TIME_ZONE = Time::Location::UTC
end

puts "\n\n runned! \n\n"
parser = Log2SQLite::LogParser.new

puts "\n\n parser=[#{ parser }] \n\n"