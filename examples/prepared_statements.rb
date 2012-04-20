#!/usr/local/bin/ruby19
# encoding: utf-8

# MosesPG -- a Ruby library for accessing PostgreSQL
# Copyright (C) 2012 James Edwin Cain (user: mosespg; domain: jcain.net)
#
# This file is part of the MosesPG library.  This Library is free software; you
# may redistribute it or modify it under the terms of the license contained in
# the file LICENCE.txt. If you did not receive a copy of the license, please
# contact the copyright holder.

require 'moses_pg'
require 'logging'
require 'optparse'

# process command line
options = {user: 'mosespg', password: 'mosespg'}
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
  "SELECT pg_sleep(0.5) AS sleep, 123::int AS t_int, 'Hello'::varchar(30) AS t_varchar",
  "SELECT pg_sleep(0.5) AS sleep, 456::int AS t_int, 'Hello'::varchar(30) AS t_varchar",
  "SELECT pg_sleep(0.5) AS sleep, 789::int AS t_int, 'Hello'::varchar(30) AS t_varchar"
]

def defer_exec(conn, sql, i, tx = nil)
  deferrable = ::EM::DefaultDeferrable.new
  defer1 = conn.prepare(sql, nil, tx)
  defer1.callback do |stmt|
    defer2 = stmt.execute(*(tx ? [tx] : []))
    defer2.callback do |result|
      puts "[#{i}] Result: #{result.inspect}"
      deferrable.succeed
    end
    defer2.errback do |err|
      puts "[#{i}] Execute failed: #{err.message}"
      deferrable.succeed
    end
  end
  defer1.errback do |err|
    puts "[#{i}] Prepare failed: #{err.message}"
    deferrable.succeed
  end
  deferrable
end

::EM.run do
  count = sqls.size * 2 # running each one twice
  ::EM.add_periodic_timer(0.2) { puts "Hello" }
  defer = MosesPG.connect(options)
  defer.callback do |conn|
    sqls.each_with_index do |sql, i|
      deferrable = defer_exec(conn, sql, i)
      deferrable.callback do
        count -= 1
        ::EM.stop if count == 0
      end
    end
    sqls.each_with_index do |sql, i|
      deferrable = conn.transaction { |tx| defer_exec(conn, sql, "#{i}t", tx) }
      deferrable.callback do
        count -= 1
        ::EM.stop if count == 0
      end
    end
  end
  defer.errback do |err|
    puts "Connection failed: #{err.message}"
    ::EM.stop
  end
end
