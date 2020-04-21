ENV['RACK_ENV'] = 'test'

require 'minitest/autorun'
require 'bulk_upsert'
require 'active_record'
require 'pg'

require 'models/person'
require 'models/post'

ActiveRecord::Base.establish_connection(
  adapter: 'postgresql',
  database: 'bulk_upsert_test',
  host: 'localhost'
)
