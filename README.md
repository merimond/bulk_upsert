# BulkUpsert

Bulkupsert allows to efficiently `INSERT` / `UPDATE` multiple ActiveRecord models in a PostgreSQL database. It **only works in PosgtreSQL**. It follows a declarative approach, just like SQL.

## Quick example

Say, you've got a big CSV file that you want to import into the database. There's a caveat though: you don't want to have duplicate records. If there's an existing record with the same ID, you don't want re-insert the record, but simply update the name. In other words, you've got a typical UPSERT task:

```csv
ID,Name
1000,ABC Corp
1001,XYZ Corp
...many-many more...
```

By default, Rails does not provide an efficient way to accomplish this. You can quickly hack a script like the one below, but it won't get you very far in terms of speed. Every row will require a `SELECT` query, followed by a `CREATE` or `UPDATE`. It's going to be excruciatingly slow.

```ruby
CSV.foreach("big.csv") do |row|
  existing = Company.find(id: row["id"])
  if existing
    existing.update(name: row["name"])
  else
    Company.create(id: row["id"], row["name"])
  end
end
```

With BulkUpsert you can speed things up by an order of magnitude (at the very least). The library will pack all data mutations into a _single_ SQL query:

```ruby
models = []
CSV.foreach("big.csv") do |row|
  model = BulkUpsert.build(Company, id: row["id"])
  model.always(:name, row["name"])
  models << model
end
BulkUpsert.save(models) # Just one SQL query here
```

## Conditional updates

The second code snippet runs a lot faster, but one could argue it's not that much shorter or easier to understand. That's true for relatively basic tasks. However, requirements tend to be a bit more complicated in real life, and that's where BulkUpsert really starts to shine.

For instance, you may want to update the `name` column only when it's `NULL`, otherwise you want to preserve the original name. Conversely, you may want to update the column only if it's defined in the file. With BulkUpsert it boils down to choosing the right keyword:

```ruby
# Always update the `name` column, regardless of its current
# value or the value of `new_name`:
model.always :name, new_name

# Update `name` column only if its current value is NULL:
model.prefer :name, new_name

# Update `name` column only if `new_name` is *not* NULL:
model.maybe :name, new_name
```

When attribute needs be updated no matter what, you can use a shorthand:

```ruby
# Specify two attribute hashes: one for search, and another for updates
model = BulkUpsert.build Company, { id: row["id"] }, { name: row["name"] }
# will produce the same result as
model = BulkUpsert.build Company, { id: row["id"] }
model.always :name, row["name"]
```

## Declarative format

TODO: add description

## Validations

TODO: add description
- mark_as_optional!

## Associations

TODO: add description
- BelongsToDeficiencyError

## Limitations

- belongs_to / BelongsToDeficiencyError
- InconsitentAttributeError
- InconsitentFlagError
- PrimaryKeyUpdateError

