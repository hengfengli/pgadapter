# PGAdapter and Ruby ActiveRecord

PGAdapter has experimental support for [ActiveRecord 8.x](https://guides.rubyonrails.org/active_record_basics.html).
This document shows how to use this sample application, and lists the
limitations when working with `ActiveRecord` with PGAdapter.

The [application.rb](application.rb) file contains a sample application using `ActiveRecord` with PGAdapter.
Use this as a reference for features of `ActiveRecord` that are supported with PGAdapter. This sample
assumes that the reader is familiar with `ActiveRecord`, and it is not intended as a tutorial for how
to use `ActiveRecord` in general.

See [Limitations](#limitations) for a full list of known limitations when working with `ActiveRecord`.

## Running the Sample
The sample application automatically starts PGAdapter and the Spanner emulator in a Docker
container. You must have Docker installed on your local system to run the sample.

```shell
bundle exec rake run
```

The database is automatically created on the emulator when the application is started. This is
handled by the `gcr.io/cloud-spanner-pg-adapter/pgadapter-emulator` Docker image. See the
[PGAdapter emulator documentation](../../../docs/emulator.md) for more information about connecting
PGAdapter to the emulator.

## Running the Sample on real Cloud Spanner
You can also run the sample on a real Cloud Spanner PostgreSQL database instead of the emulator.
The sample application will automatically start PGAdapter in a Docker container and connect to your
Cloud Spanner database. For this, you need to follow these configuration steps

1. Set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to point to a valid (service) account file
   that should be used to run the sample.
2. Modify the `config/database.yml` file so it points to your database.

Alternatively, you can also set the following environment variables to configure the database and port number:

```shell
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
PGDATABASE=projects/my-project/instances/my-instance/databases/my-database
```

Make sure the database that you point the sample to is empty, as the sample application will
automatically create the required data model in that database.

Run the sample on Cloud Spanner with the following command:

```shell
bundle exec rake run_prod
```


## Configuration
You need to add some extra options to your Ruby ActiveRecord configuration to work with Cloud Spanner.

The following code snippets show the additional options that must be set when working with PGAdapter
and Cloud Spanner. These options ensure that:
1. ActiveRecord uses `timestamptz` instead of `timestamp(6)` as the default date/time data type.
2. `advisory_locks` are not used for migrations.
3. DDL transactions are converted to DDL batches. See [DDL options](../../../docs/ddl.md) for more information.
4. `pg_class` and related tables are emulated by PGAdapter.
5. ActiveRecord schema dumps after migrations are disabled. This configuration is set in the `database.yml` file.

### Initialization
The following initialization code makes sure that ActiveRecord will use `timestamptz` as the default
date/time type for PostgreSQL databases. In addition, it will prevent ActiveRecord from using type
modifiers for `timestamptz` data types (e.g. `timestamptz(6)`).

This initialization code is only required for executing migrations. You may omit this from your
initialization code when you are not running migrations.

This sample application has this initialization code in the `config/environment.rb` file.

```ruby
# Make sure that the PostgreSQL-adapter uses timestamptz without any type modifiers.
ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.datetime_type = :timestamptz

module ActiveRecord::ConnectionAdapters
  class PostgreSQLAdapter
    def supports_datetime_with_precision?
      false
    end
  end
end
```

__NOTE__: The `datetime_type` option was added in ActiveRecord 7.0.
Use the following configuration for ActiveRecord 6.1 or lower:

```ruby
ActiveRecord::ConnectionAdapters::PostgreSQLAdapter::NATIVE_DATABASE_TYPES[:datetime] = { name: "timestamptz" }

module ActiveRecord::ConnectionAdapters
   class PostgreSQLAdapter
      def supports_datetime_with_precision?
         false
      end
   end
end
```

### database.yml
The following is the `database.yml` that is used by this sample. It uses the standard
`PGHOST`, `PGPORT` and `PGDATABASE` environment variables. Note that the sample application
starts PGAdapter automatically on a random port, and the `PGPORT` environment variable is
automatically updated to use this port.

```yaml
default: &default
  adapter: postgresql
  host: <%= ENV['PGHOST'] || "localhost" %>
  port: <%= ENV['PGPORT'] || "5432" %>
  database: <%= ENV['PGDATABASE'] || "projects/my-project/instances/my-instance/databases/my-database" %>
  pool: 5
  # Advisory locks are not supported by PGAdapter
  advisory_locks: false
  # Schema dumping uses pg_catalog tables and functions that are not supported by Spanner.
  schema_dump: false
  # These settings ensure that migrations and schema inspections work.
  variables:
    "spanner.ddl_transaction_mode": "AutocommitExplicitTransaction"
    "spanner.emulate_pg_class_tables": "true"

development:
  <<: *default
```

## Data Model
The data model is automatically created using a standard ActiveRecord migration script.
See [db/migrate/01_create_tables.rb](db/migrate/01_create_tables.rb) for the migration script.

You can drop the data model again using the `drop_data_model.sql` script in this folder.


## Limitations
The following limitations are currently known:

| Limitation                     | Workaround                                                                                                                                                                                                                                                                                                                                               |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Schema Dumper                  | Cloud Spanner does not support all PostgreSQL `pg_catalog` tables. Using `ActiveRecord::SchemaDumper` to get export the current schema is not guaranteed to include all objects in the database.                                                                                                                                                         |
| DDL Transactions               | Cloud Spanner does not support DDL statements in a transaction. Add `"spanner.ddl_transaction_mode": "AutocommitExplicitTransaction"` to the `variables` section in your `database.yml` file to automatically convert DDL transactions to [non-atomic DDL batches](../../../docs/ddl.md). See [config/database.yml](config/database.yml) for an example. |
| Generated primary keys         | The `serial` data type is not supported. Use bit-reversed sequences to auto-generate primary keys.                                                                                                                                                                                                                                                       |
| Upsert                         | `upsert` and `upsert_all` are not supported.                                                                                                                                                                                                                                                                                                             |
| SELECT ... FOR UPDATE          | Pessimistic locking / `SELECT ... FOR UPDATE` is not supported.                                                                                                                                                                                                                                                                                          |
| Transaction isolation level    | Only isolation level `:serializable` is supported.                                                                                                                                                                                                                                                                                                       |

### Schema Dumper
Dumping the schema of a database is not guaranteed to produce a complete result. There is currently
no workaround for this limitation. It is recommended to disable automatic schema dumps after a migration.

### Generated Primary Keys
The `serial` data type is currently not supported.
Generated primary keys can however be used in combination with bit-reversed sequences.

The [TicketSale](models/ticket_sale.rb) model in this sample uses an auto-generated primary key.
The [table definition](db/migrate/01_create_tables.rb) for this model looks like this:

```ruby
execute "create sequence ticket_sale_seq
         bit_reversed_positive
         skip range 1 1000
         start counter with 50000;"

create_table :ticket_sales, id: false, primary_key: :id do |t|
   t.column :id, "bigint not null primary key default nextval('ticket_sale_seq')"
   t.references :concert, foreign_key: {primary_key: :concert_id},
                type: :string, limit: 36, index: false
   t.string :customer_name
   t.numeric :price
   t.string :seats, array: true
   t.datetime :created_at
   t.datetime :updated_at
   t.integer :lock_version, null: false
end
```

See https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots
for more information on choosing a good primary key. Most models in this sample use
random UUIDs that are generated by the client and stored as strings for primary keys.

Example:

```ruby
Singer.create({singer_id: SecureRandom.uuid,
               first_name: 'Bob',
               last_name: 'Anderson'})
```

### Upsert / ON CONFLICT Clauses
`INSERT ... ON CONFLICT ...` are not supported by Cloud Spanner and should not be used. Trying to
use `upsert` or `upsert_all` will fail.

### Pessimistic Locking - SELECT ... FOR UPDATE
Locking clauses, like `SELECT ... FOR UPDATE`, are not supported (see also https://api.rubyonrails.org/classes/ActiveRecord/Locking/Pessimistic.html).
These are normally also not required, as Cloud Spanner uses isolation level `serializable` for
read/write transactions.
