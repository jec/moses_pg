# encoding: utf-8

#--
# MosesPG -- a Ruby library for accessing PostgreSQL
# Copyright (C) 2012 James Edwin Cain (user: mosespg; domain: jcain.net)
#
# This file is part of the MosesPG library.  This Library is free software; you
# may redistribute it or modify it under the terms of the license contained in
# the file LICENCE.txt. If you did not receive a copy of the license, please
# contact the copyright holder.
#++

require 'moses_pg/sync'
require 'logging'

$dbname = ENV['PGDBNAME']
$user = ENV['PGUSER'] || 'mosespg'
$password = ENV['PGPASSWORD'] || 'mosespg'
if ENV['PGVERBOSE']
  Logging.init(%w{trace debug info warn error fatal})
  $logger = Logging.logger[MosesPG::Connection]
  $logger.appenders = Logging.appenders.stdout(layout: Logging::Layouts::Pattern.new(pattern: "%m\n"))
  $logger.level = :trace
else
  $logger = nil
end

module MosesPG

  describe Statement do

    def stop_em_on_error
      begin
        yield
      rescue Exception => e
        example.set_exception(e)
        ::EM.stop
      end
    end

    context 'when run with EM::run' do
      around(:each) do |example|
        EM.run do
          @conn_defer = Connection.connect(dbname: $dbname, user: $user, password: $password, logger: $logger)
          @conn_defer.callback do |conn|
            @conn = conn
            example.run
          end
          @conn_defer.errback do |errmsg|
            raise "Connection failed: #{errmsg}"
            ::EM.stop
          end
        end
      end

      describe '::prepare' do
        it 'returns a Statement' do
          stop_em_on_error do
            defer = Statement.prepare(@conn, "SELECT $1::bool, $2::bytea, $3::real")
            defer.callback do |stmt|
              stmt.should be_instance_of(Statement)
              ::EM.stop
            end
            defer.errback { |errstr| fail errstr }
          end
        end
      end

      describe '#execute' do
        context 'when preceded by #bind' do
          it 'returns the result' do
            stop_em_on_error do
              defer = Statement.prepare(@conn, "SELECT $1::bool AS t_bool, $2::bytea AS t_bytea, $3::real AS t_real")
              defer.callback do |stmt|
                defer1 = stmt.execute('t', 'Hello world', 123.456)
                defer1.callback do |result|
                  result.columns.collect { |c| c.name }.should == %w{t_bool t_bytea t_real}
                  result.rows_as_native.should == [[true, 'Hello world', 123.456]]
                  ::EM.stop
                end
                defer1.errback { |errstr| fail errstr }
              end
              defer.errback { |errstr| fail errstr }
            end
          end
        end
      end
    end

  end

end
