# MosesPG

MosesPG provides EventMachine-based access to PostgreSQL using version 3.0 of
their frontend/backend protocol. It is still under active development and
incomplete. In particular, type translation is implemented for most of the
column data types described in the PostgreSQL manual, but many of the more
exotic types do not yet have meaningful translations. Simple queries and
multi-step queries (parse, bind/describe/execute) are working.

MosesPG is currently being developed as pure Ruby, but this is not strictly a
goal of the project. Eventually, if it is required for performance, parts will
be coded as C extensions.

## Dependencies

* Ruby 1.9

* [Eventmachine](https://github.com/eventmachine/eventmachine)

* [state_machine](https://github.com/pluginaweek/state_machine)

## Features

### Implemented features

* Single-threaded, event-driven access to PostgreSQL, with notification of
completed queries via EventMachine&rsquo;s Deferrables

* Built-in per-connection serialization of queries (no need to wait for one
query to complete before submitting another)

* Translation to native Ruby types (though not all types yet have meaningful
translations)

### Planned features

* Declaration of parameter types using `Datatype` values rather than
  the raw OIDs

* SSL session encryption

* Function call interface

* COPY in/out operations

## Examples

### Simple queries

Simple queries use the `#execute` method and receive a `Result` through the `#callback`.

```ruby
require 'eventmachine'
require 'moses_pg'

EM.run do
  defer = MosesPG.connect(user: 'jim', password: 'jim')
  defer.callback do |conn|
    defer1 = conn.execute("SELECT 'Hello World!' AS hello, 954 AS area_code, localtimestamp AS now")
    defer1.callback do |result|
      result.first.columns.each { |c| puts c }
      result.first.each_row_as_native { |r| p r }
      EM.stop
    end
    defer1.errback { |errstr| puts "ERROR: #{errstr}"; EM.stop }
  end
  defer.errback do |errstr|
    puts "Connection failed: #{errstr}"
    EM.stop
  end
end
```

_produces:_

    #<MosesPG::Column name="hello" type=#<MosesPG::Datatype::Varchar precision=30> format=0>
    #<MosesPG::Column name="area_code" type=#<MosesPG::Datatype::Integer > format=0>
    #<MosesPG::Column name="now" type=#<MosesPG::Datatype::Timestamp precision=6> format=0>
    ["Hello World!", 954, 2012-03-14 23:26:33 -0400]

Note that the last column has been translated to a Ruby Time object.

### Prepared statements

For prepared statements, use `#prepare` to parse and store the SQL, and `#execute_prepared`
to run it later.

```ruby
EM.run do
  defer = MosesPG.connect(user: 'jim', password: 'jim')
  defer.callback do |conn|
    defer1 = conn.prepare('stmt1', "SELECT $1::varchar(30) AS hello, $2::int AS area_code, $3::timestamp AS now")
    defer1.callback do
      defer2 = conn.execute_prepared('stmt1', 'Hello world!', 954, Time.now)
      defer2.callback do |result|
        result.first.columns.each { |c| puts c }
        result.first.each_row_as_native { |r| p r }
        EM.stop
      end
      defer2.errback { |errstr| puts "ERROR: #{errstr}"; EM.stop }
    end
    defer1.errback { |errstr| puts "ERROR: #{errstr}"; EM.stop }
  end
  defer.errback do |errstr|
    puts "Connection failed: #{errstr}"
    EM.stop
  end
end
```

_produces:_

## License

MosesPG is licensed under the three-clause BSD license.

## What&rsquo;s up with the name?

I was trying to avoid naming collisions with other projects. Moses is my Boxer
dog.
