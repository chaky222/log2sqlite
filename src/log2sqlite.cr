require "db"
require "sqlite3"
require "sqlite3"
require "../libs/**"
require "./log2sqlite/**"

puts "\n\n runned3! \n\n"
params = {} of String => String
ARGV.each do |x|
  arr = x.split('=')
  params["#{ arr.first }"] = arr[1..-1].join('=')
end
parser = Log2SQLite::LogParser.new(params).run

puts "\n\n parser=[#{ parser }] \n\n"