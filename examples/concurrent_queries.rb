#!/usr/local/bin/ruby19

require 'moses_pg'
require 'logging'
require 'optparse'

# process command line
options = {user: 'jim', password: 'jim'}
OptionParser.new do |opt|
  opt.on('-H HOST', 'Connect to PostgreSQL on HOST (default: unix socket)') { |h| options[:host] = h }
  opt.on('-P PORT', Integer, 'Connect to PostgreSQL on PORT (default: 5432)') { |p| options[:port] = p }
  opt.on('-u USER', "Connect as USER (default: #{options[:user]})") { |u| options[:user] = u }
  opt.on('-p PASSWORD', "Connect using PASSWORD (default: #{options[:password]})") { |p| options[:password] = p }
  opt.on('-v', 'Show verbose logging from the library') do
    Logging.init(%w{trace debug info warn error fatal})
    logger = Logging.logger[MosesPG::Connection]
    logger.appenders = Logging.appenders.stdout(layout: Logging::Layouts::Pattern.new(pattern: "%m\n"))
    logger.level = :trace
    options[:logger] = logger
  end
end.parse!

sqls = [
  "select pg_sleep(1) as sleep, 1::int as t_int, 2::bigint as t_bigint, 3::smallint as t_smallint",
  "select pg_sleep(1) as sleep, 1::decimal as t_decimal, 12.34::numeric as t_numeric, 12.34::real as t_real",
  "select pg_sleep(1) as sleep, '2012-01-02'::date, '12:34:56.789'::time, interval '1 day 01:23:45.678'",
  "select pg_sleep(1) as sleep, timestamp '2012-01-02 12:34:56.789' as t_ts, timestamp with time zone '2012-01-02 12:34:56.789-08' as t_ts_tz",
  "select pg_sleep(1) as sleep, TRUE::bool as t_bool1, FALSE::bool as t_bool2"
]

::EM.run do
  # say hello periodically
  puts 'Hello'
  ::EM.add_periodic_timer(0.2) { puts "Hello" }

  count = sqls.size
  deferrable, db = MosesPG.connect(options)

  deferrable.errback do |errstr|
    puts "Connection failed: #{errstr}"
    ::EM.stop
  end

  deferrable.callback do
    puts 'Connected'
    sqls.each_with_index do |sql, i|
      defer = db.execute(sql)
      defer.callback do |result|
        puts "Query #{i} done: #{result.inspect}"
        count -= 1
        ::EM.stop if count == 0
      end
      defer.errback do |errstr, result|
        puts "Query #{i} failed: #{errstr}; partial result: #{result.inspect}"
        count -= 1
        ::EM.stop if count == 0
      end
    end
  end
end
