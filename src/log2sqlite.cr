require "db"
require "./log2sqlite/**"

module Log2SQLite
  DATE_FORMAT = "%F %H:%M:%S.%L"
  # :nodoc:
  TIME_ZONE = Time::Location::UTC
end