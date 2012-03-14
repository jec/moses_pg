# MosesPG

MosesPG provides EventMachine-based access to PostgreSQL using version 3 of
their wire protocol. It is still under active development and incomplete. In
particular, type translation has not yet been implemented, but simple queries
and multi-step queries (parse, bind, execute) are working.

```ruby
require 'eventmachine'
require 'moses_pg'

EM.run do
  defer = MosesPG.connect(user: 'jim', password: 'jim')
  defer.callback do |conn|
    defer1 = conn.execute("SELECT 'Hello World!' AS hello")
    defer1.callback { |result| p result; EM.stop }
    defer1.errback { |errstr| puts "ERROR: #{errstr}"; EM.stop }
  end
  defer.errback do |errstr|
    puts "Connection failed: #{errstr}"
    EM.stop
  end
end
```

_produces:_

`
[#<MosesPG::Result:0x00000102884520 @rows=[["Hello World!"]], @columns=[["hello", 0, 0, 705, -2, -1, 0]], @tag="SELECT 1">]
`

The column metadata will eventually be returned in a more useful form.

## Dependencies

* Ruby 1.9

* [Eventmachine](https://github.com/eventmachine/eventmachine)

* [state_machine](https://github.com/pluginaweek/state_machine)

## Design goals

Once completed, MosesPG will have the following features:

* Single-threaded, event-driven access to PostgreSQL, with notification of
completed queries via EventMachine&rsquo;s Deferrables

* Built-in per-connection serialization of queries (no need to wait for one
query to complete before submitting another)

* Translation to native Ruby types

## Anti-design goals

MosesPG is currently being developed as pure Ruby, but this is not strictly a
goal of the project. Eventually, if it is required for performance, parts will
be coded as C extensions.

## What&rsquo;s up with the name?

I was trying to avoid naming collisions with other projects. Moses is my Boxer
dog.
