Gem::Specification.new do |s|
  s.name    = "bulk_upsert"
  s.version = "1.0.0"
  s.summary = "Insert and update records within just one SQL query"
  s.authors = ["Alex Serebryakov"]
  s.files   = Dir['lib/**/*.rb']
  s.add_development_dependency('pg')
  s.add_development_dependency('activerecord')
end
