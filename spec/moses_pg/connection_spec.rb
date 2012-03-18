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

  describe Connection do

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

      describe 'simple queries' do
        context 'when given an invalid statement' do
          it 'returns an error' do
            defer = @conn.execute("SELECTx 1")
            defer.errback do |errstr|
              errstr.should match(/syntax error/i)
              ::EM.stop
            end
            defer.callback { |results| fail '#callback should not be called' }
          end
        end

        context 'when given valid SELECT statements' do
          it 'returns an array of Results, one for each query' do
            defer = @conn.execute("SELECT 12345::int AS t_int, 123456789012::bigint AS t_bigint, 123::smallint AS t_smallint")
            defer.callback do |results|
              results.size.should == 1
              rows = results.first.rows
              rows.size.should == 1
              rows.first.should == ["12345", "123456789012", "123"]
              results.first.each_row_as_native do |row|
                row.should == [12345, 123456789012, 123]
              end
              results.first.columns.collect { |c| c.name }.should == %w{t_int t_bigint t_smallint}

              defer1 = @conn.execute("SELECT 12345::int AS t_int; SELECT 123456789012::bigint AS t_bigint")
              defer1.callback do |results|
                results.size.should == 2
                results.collect { |r| r.rows }.should == [[['12345']], [['123456789012']]]
                ::EM.stop
              end
              defer1.errback { |errstr| fail errstr }
            end
            defer.errback { |errstr| fail errstr }
          end
        end
      end

      describe 'extended queries' do
        context 'when given an invalid statement' do
          it 'returns an error' do
            stop_em_on_error do
              defer = @conn.prepare("SELECTx 12345::int AS t_int")
              defer.errback do |errstr|
                errstr.should match(/syntax error/i)
                ::EM.stop
              end
              defer.callback { |results| fail '#callback should not be called' }
            end
          end
        end

        context 'when given a valid SELECT statement' do
          it 'returns the query results' do
            stop_em_on_error do
              defer = @conn.prepare("SELECT $1::int AS t_int, $2::varchar(30) AS t_varchar")
              defer.callback do |stmt|
                defer1 = stmt.execute(12345, 'This is a test')
                defer1.callback do |result|
                  result.rows.should == [['12345', 'This is a test']]
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

    context 'when run with EM::synchrony' do
      around(:each) do |example|
        EM.synchrony do
          @conn = Connection.connect!(dbname: $dbname, user: $user, password: $password, logger: $logger)
          example.run
          ::EM.stop
        end
      end

      describe 'simple queries' do
        after(:each) do
          begin
            @conn.execute!("DROP TABLE alpha")
          rescue MosesPG::Error
            # ignore
          end
        end
        context 'when given a CREATE TABLE statement with implicit index' do
          it 'returns the notices in the Result' do
            results = @conn.execute!("CREATE TABLE alpha (id SERIAL)")
            results.first.notices.first['Message'].should match(/create implicit sequence/)
          end
        end
      end
    end
  end

end
