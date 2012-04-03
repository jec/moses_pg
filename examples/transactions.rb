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

SQLS = [
  ["select pg_sleep(1) as sleep, 1::int as t_int", "select pg_sleep(1) as sleep, 'a'::char(1) as t_char"],
  ["select pg_sleep(1) as sleep, 2::int as t_int", "select pg_sleep(1) as sleep, 'b'::char(1) as t_char"],
  ["select pg_sleep(1) as sleep, 3::int as t_int", "select pg_sleep(1) as sleep, 'c'::char(1) as t_char"],
  ["select pg_sleep(1) as sleep, 4::int as t_int", "select pg_sleep(1) as sleep, 'd'::char(1) as t_char"]
]

::EM.run do
  # say hello periodically
  puts 'Hello'
  ::EM.add_periodic_timer(0.2) { puts "Hello" }

  deferrable = MosesPG.connect(options)

  deferrable.errback do |errstr|
    puts "Connection failed: #{errstr}"
    ::EM.stop
  end

  deferrable.callback do |db|
    puts 'Connected'
    master_count = SQLS.size
    SQLS.each_with_index do |sqls, txnum|
      tx_deferrable = db.transaction do |tx|
        puts "Start transaction #{txnum}".center(79, '-')
        deferrable = ::EM::DefaultDeferrable.new
        count = sqls.size
        sqls.each_with_index do |sql, i|
          defer = tx.execute(sql)
          defer.callback do |result|
            puts "Transaction #{txnum} query #{i} done: #{result.inspect}"
            count -= 1
            deferrable.succeed if count == 0
          end
          defer.errback do |err, result|
            puts "Transaction #{txnum} query #{i} failed: #{err.message}; partial result: #{result.inspect}"
            count -= 1
            deferrable.succeed if count == 0
          end
        end
        deferrable
      end
      tx_deferrable.callback do
        puts "End transaction #{txnum}".center(79, '-')
        master_count -= 1
        ::EM.stop if master_count == 0
      end
      tx_deferrable.errback do |err|
        puts "Transaction #{txnum} failed: #{err.message}"
        puts "End transaction #{txnum}".center(79, '-')
        master_count -= 1
        ::EM.stop if master_count == 0
      end
    end
  end
end
