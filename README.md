# MosesPG

MosesPG provides EventMachine-based access to PostgreSQL using version 3.0 of
their frontend/backend protocol. It is still under active development and
incomplete. In particular, type translation is implemented for most of the
column data types described in the PostgreSQL manual, but many of the more
exotic types do not yet have meaningful translations. Simple queries and
multi-step queries (parse, bind/describe/execute) are working on a basic level,
but mixing and matching many different query types all at once may not be
stable.

MosesPG is currently being developed as pure Ruby, but this is not strictly a
goal of the project. Eventually, if it is required for performance, I would
consider coding parts as C extensions.

### Status Update 22 May 2012

The project team have decided to use a backend other than PostgreSQL. As a
result, at this time, MosesPG is on my back burner. I hope to return to it soon
with an upcoming project that will likely be using PostgreSQL.

## Dependencies

* Ruby 1.9

* [EventMachine](https://github.com/eventmachine/eventmachine)

* [EM-Synchrony](https://github.com/igrigorik/em-synchrony)

* [state_machine](https://github.com/pluginaweek/state_machine)

## Features

### Implemented features

* Single-threaded, event-driven access to PostgreSQL, with notification of
  completed queries via EventMachine&rsquo;s Deferrables

* Optional synchronous query methods (e.g. `#execute!` instead of `#execute`)
  that block until completion

* Built-in per-connection serialization of queries and transactions (no need to
  wait for one query or transaction to complete before submitting another)

* Translation to native Ruby types (though not all types yet have meaningful
  translations)

## Examples

### Simple queries

Simple queries use the `#execute` method and receive a `Result` through the `#callback`.

```ruby
require 'moses_pg'

EM.run do
  defer = MosesPG.connect(user: 'mosespg', password: 'mosespg')
  defer.callback do |conn|
    defer1 = conn.execute("SELECT 'Hello World!' AS hello, 954 AS area_code, localtimestamp AS now")
    defer1.callback do |result|
      result.first.columns.each { |c| puts c }
      result.first.each_row_as_native { |r| p r }
      EM.stop
    end
    defer1.errback { |err| puts "ERROR: #{err.message}"; EM.stop }
  end
  defer.errback do |err|
    puts "Connection failed: #{err.message}"
    EM.stop
  end
end
```

_produces:_

    #<MosesPG::Column name="hello", type=MosesPG::Datatype::Varchar_30, format=0>
    #<MosesPG::Column name="area_code", type=MosesPG::Datatype::Integer, format=0>
    #<MosesPG::Column name="now", type=MosesPG::Datatype::Timestamp_6, format=0>
    ["Hello World!", 954, 2012-04-21 00:30:23 -0400]

Note that the last column has been translated to a Ruby Time object.

### Prepared statements

For prepared statements, use `Connection#prepare` to parse the SQL and get a
`Statement` object. Use `Statement#execute` to run it later.

```ruby
require 'moses_pg'

EM.run do
  defer = MosesPG.connect(user: 'mosespg', password: 'mosespg')
  defer.callback do |conn|
    defer1 = conn.prepare("SELECT $1::varchar(30) AS hello, $2::int AS area_code, $3::timestamp AS now")
    defer1.callback do |stmt|
      defer2 = stmt.execute('Hello world!', 954, Time.now)
      defer2.callback do |result|
        result.columns.each { |c| puts c }
        result.each_row_as_native { |r| p r }
        EM.stop
      end
      defer2.errback { |err| puts "ERROR: #{err.message}"; EM.stop }
    end
    defer1.errback { |err| puts "ERROR: #{err.message}"; EM.stop }
  end
  defer.errback do |err|
    puts "Connection failed: #{err.message}"
    EM.stop
  end
end
```

_produces:_

    #<MosesPG::Column name="hello", type=MosesPG::Datatype::Varchar_30, format=0>
    #<MosesPG::Column name="area_code", type=MosesPG::Datatype::Integer, format=0>
    #<MosesPG::Column name="now", type=MosesPG::Datatype::Timestamp_6, format=0>
    ["Hello world!", 954, 2012-04-21 00:33:42 -0400]

### Synchronous queries

Optional methods are available with a bang (!) suffix that block until the
query is completed. Instead of a `Deferrable`, they return the data that would
be received by the `#callback`.  To use these, `require 'moses_pg/sync'`. The
following code is equivalent to the example above and outputs the same result.

```ruby
require 'moses_pg/sync'

EM.synchrony do
  conn = MosesPG.connect!(user: 'mosespg', password: 'mosespg')
  stmt = conn.prepare!("SELECT $1::varchar(30) AS hello, $2::int AS area_code, $3::timestamp AS now")
  result = stmt.execute!('Hello world!', 954, Time.now)
  result.columns.each { |c| puts c }
  result.each_row_as_native { |r| p r }
  EM.stop
end
```

## License

MosesPG is licensed under the three-clause BSD license (see LICENSE.txt).

## What&rsquo;s up with the name?

I was trying to avoid naming collisions with other projects. Moses is my Boxer
dog.
