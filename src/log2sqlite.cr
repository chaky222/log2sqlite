require "db"
require "sqlite3"
require "./log2sqlite/**"

module Log2SQLite
  DATE_FORMAT = "%F %H:%M:%S.%L"
  # :nodoc:
  TIME_ZONE = Time::Location::UTC
end

puts "\n\n runned3! \n\n"
params = {} of String => String
ARGV.each do |x|
  arr = x.split('=')
  params["#{ arr.first }"] = arr[1..-1].join('=')
end
parser = Log2SQLite::LogParser.new(params).run

puts "\n\n parser=[#{ parser }] \n\n"