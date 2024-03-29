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

require 'state_machine'
require 'moses_pg/transaction'

module MosesPG

  #
  # Encapsulates a prepared statement
  #
  # Prepared statements can be created in one of two ways: +Statement::prepare+
  # or +Connection#prepare+.
  #
  class Statement

    state_machine :state, :initial => :prepared do
      event :describe_statement_sent do
        transition :prepared => :describe_statement_in_progress
      end
      event :bind_sent do
        transition [:statement_described, :bound, :executed] => :bind_in_progress
      end
      event :execute_sent do
        transition [:statement_described, :bound, :executed] => :execute_in_progress
      end
      event :close_portal_sent do
        transition [:bound, :executed] => :close_portal_in_progress
      end
      event :close_sent do
        transition [:statement_described, :bound, :executed] => :close_in_progress
      end
      event :action_completed do
        transition :describe_statement_in_progress => :statement_described
        transition :bind_in_progress => :bound
        transition :execute_in_progress => :executed
        transition :close_portal_in_progress => :statement_described
        transition :close_in_progress => :closed
      end
      event :action_failed do
        transition [:describe_statement_in_progress, :bind_in_progress, :execute_in_progress, :close_in_progress] => :prepared
      end

      state :prepared do
        def describe(tx = nil)
          deferrable = ::EM::DefaultDeferrable.new
          defer1 = @connection._describe_statement(self, tx)
          describe_statement_sent
          defer1.callback do |result|
            action_completed
            @parameters = result.parameters
            @columns = result.columns
            deferrable.succeed
          end
          defer1.errback do |err|
            action_failed
            deferrable.fail(err)
          end
          deferrable
        end
      end

      state :statement_described do
        def close_portal(tx = nil)
          deferrable = ::EM::DefaultDeferrable.new
          deferrable.succeed
          deferrable
        end
      end

      state :bound, :executed do
        def close_portal(tx = nil)
          deferrable = ::EM::DefaultDeferrable.new
          if @last_tx
            # if the last bind was in a transaction, the portal was (or will
            # be) closed at the end of that transaction
            deferrable.succeed
          else
            defer1 = @connection._close_portal(self, tx)
            close_portal_sent
            defer1.callback do
              action_completed
              deferrable.succeed
            end
            defer1.errback do |err|
              action_failed
              deferrable.fail(err)
            end
          end
          deferrable
        end
      end

      state :statement_described, :bound, :executed do
        def _bind(tx, *bindvars)
          deferrable = ::EM::DefaultDeferrable.new
          # close the previous portal if there is one, to release the
          # resources on the backend
          defer1 = close_portal(tx)
          defer1.callback do
            @portal_name = generate_portal_name
            @last_tx = tx
            defer2 = @connection._bind(self, bindvars, tx)
            bind_sent
            defer2.callback do
              action_completed
              deferrable.succeed
            end
            defer2.errback do |err|
              action_failed
              deferrable.fail(err)
            end
          end
          defer1.errback { |err| deferrable.fail(err) }
          deferrable
        end

        def execute(*bindvars)
          deferrable = ::EM::DefaultDeferrable.new
          tx = bindvars.first.kind_of?(Transaction) ? bindvars.shift : nil
          # We have to run bind first every time, because executing an existing
          # port does not start a select statement over again. Also, the state
          # machine in Connection expects the sequence for every prepared
          # statement to be bind and then execute.
          defer1 = _bind(tx, *bindvars)
          defer1.callback do
            defer2 = @connection._execute(self, tx)
            execute_sent
            defer2.callback do |result|
              action_completed
              result.columns = @columns
              deferrable.succeed(result)
            end
            defer2.errback do |err|
              action_failed
              deferrable.fail(err)
            end
          end
          defer1.errback do |err|
            action_failed
            deferrable.fail(err)
          end
          deferrable
        end

        def close(tx = nil)
          deferrable = @connection._close_statement(self, tx)
          close_sent
          deferrable.callback { action_completed }
          deferrable.errback { action_failed }
          deferrable
        end
      end
    end

    @@stmt_seq = 0

    private_class_method :new

    # Returns the name of the prepared statement as stored in PostgreSQL's session
    # @return [String]
    attr_reader :name

    # Returns the name of the bound portal as stored in PostgreSQL's session
    # @return [String]
    attr_reader :portal_name

    # Returns the +Column+s to be output upon execution, or +nil+ if not yet bound
    # @return [Array<MosesPG::Column>]
    attr_reader :columns

    # Returns the +Datatype+ classes for the required parameters
    # @return [Array<Class>]
    attr_reader :parameters

    # Returns the +Connection+ to which this +Statement+ belongs
    # @return [MosesPG::Connection]
    attr_reader :connection

    #
    # Initiates the preparation of a SQL statement and returns a +Deferrable+
    #
    # Upon successful parsing, it returns the new +Statement+ through the
    # +Deferrable+'s +#callback+ method.
    #
    # @param [MosesPG::Connection] connection The +Connection+ in which to prepare the SQL
    # @param [String] sql The SQL text to prepare
    # @param [Array<Integer>] datatypes 0 for text and 1 for binary
    # @return [EventMachine::Deferrable]
    #
    def self.prepare(connection, sql, datatypes = nil, tx = nil)
      deferrable = ::EM::DefaultDeferrable.new
      name = generate_statement_name
      defer1 = connection._prepare(name, sql, datatypes, tx)
      defer1.callback do
        stmt = new(connection, sql, name)
        defer2 = stmt.describe(tx)
        defer2.callback { deferrable.succeed(stmt) }
        defer2.errback { |err| deferrable.fail(err) }
      end
      defer1.errback { |err| deferrable.fail(err) }
      deferrable
    end

    def initialize(connection, sql, name)
      super()
      @connection = connection
      @sql = sql
      @name = name
      @port_seq = 0
    end

    #
    # Initiates the execution step for the +Statement+ and returns a
    # +Deferrable+
    #
    # If the +Statement+ was not previously bound, or if +bindvars+ are given,
    # then the method calls +#_bind+ and waits for completion before stating the
    # execution.
    #
    # Upon successful completion, the +Result+ object is passed through the
    # +Deferrable+'s +#callback+.
    #
    # @param [Array<Object>] bindvars The values being bound
    # @return [EventMachine::Deferrable]
    #
    def execute(*bindvars)
      super
    end

    def result_format_codes
      if @columns
        @columns.collect { |col| col.type.result_format_code }
      else
        []
      end
    end

    #
    # Initiates the closing of the +Statement+ and returns a +Deferrable+
    #
    # Upon successful completion, the +Deferrable+'s +#callback+ is called with
    # no arguments.
    #
    # @return [EventMachine::Deferrable]
    #
    def close
      super
    end

    #
    # @return [String]
    #
    def to_s
      "#<#{self.class.name} state=#{@state.inspect} sql=#{@sql.inspect} name=#{@name.inspect}>"
    end

    private

    #
    # Initiates the bind step for the +Statement+ and returns a +Deferrable+
    #
    # Upon successful completion, the +Deferrable+'s +#callback+ is called with
    # no arguments.
    #
    # @param [nil, MosesPG::Transaction] tx The active +Transaction+ if any
    # @param [Array<Object>] bindvars The values being bound
    # @return [EventMachine::Deferrable]
    #
    def _bind(tx, *bindvars)
      super
    end

    def self.generate_statement_name
      @@stmt_seq += 1
      "stmt_#{@@stmt_seq.to_s(16)}"
    end

    def generate_portal_name
      @port_seq += 1
      "port_#{name[5..-1]}_#{@port_seq.to_s(16)}"
    end

  end

end
