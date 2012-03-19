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

module MosesPG

  #
  # Encapsulates a prepared statement
  #
  # Prepared statements can be created in one of two ways: +Statement::prepare+
  # or +Connection#prepare+.
  #
  class Statement

    state_machine :state, :initial => :prepared do
      event :bind_sent do
        transition [:prepared, :bound, :executed] => :bind_in_progress
      end
      event :describe_sent do
        transition :bound => :describe_in_progress
      end
      event :execute_sent do
        transition [:prepared, :bound, :executed] => :execute_in_progress
      end
      event :close_sent do
        transition [:prepared, :bound, :executed] => :close_in_progress
      end
      event :action_completed do
        transition :bind_in_progress => :bound
        transition :describe_in_progress => :described
        transition :execute_in_progress => :executed
        transition :close_in_progress => :closed
      end
      event :action_failed do
        transition [:bind_in_progress, :describe_in_progress, :execute_in_progress, :close_in_progress] => :prepared
      end

      state :prepared, :bound, :executed do
        def bind(*bindvars)
          # TODO: close the previous portal if there is one, to release the
          # resources on the backend
          @portal_name = generate_portal_name
          deferrable = ::EM::DefaultDeferrable.new
          defer1 = @connection._bind(self, bindvars)
          bind_sent
          defer1.callback do
            action_completed
            defer2 = @connection._describe_portal(self)
            describe_sent
            defer2.callback do |result|
              action_completed
              @columns = result.columns
              deferrable.succeed
            end
            defer2.errback do |errstr|
              action_failed
              deferrable.fail(errstr)
            end
          end
          defer1.errback do |errstr|
            action_failed
            deferrable.fail(errstr)
          end
          deferrable
        end

        def execute(*bindvars)
          if prepared?
            deferrable = ::EM::DefaultDeferrable.new
            defer1 = bind(*bindvars)
            defer1.callback do
              defer2 = @connection._execute(self)
              execute_sent
              defer2.callback do |result|
                action_completed
                result.columns = @columns
                deferrable.succeed(result)
              end
              defer2.errback do |errstr|
                action_failed
                deferrable.fail(errstr)
              end
            end
            defer1.errback do |errstr|
              action_failed
              deferrable.fail(errstr)
            end
            deferrable
          else
            @connection._execute(self)
          end
        end

        def close
          deferrable = @connection._close_statement(self)
          close_sent
          deferrable.callback { action_completed }
          deferrable.errback { action_failed }
          deferrable
        end
      end
    end

    private_class_method :new

    # Returns the name of the prepared statement as stored in PostgreSQL's session
    #
    # @return [String]
    attr_reader :name

    # Returns the name of the bound portal as stored in PostgreSQL's session
    #
    # @return [String]
    attr_reader :portal_name

    # Returns the +Connection+ to which this +Statement+ belongs
    #
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
    def self.prepare(connection, sql, datatypes = nil)
      deferrable = ::EM::DefaultDeferrable.new
      name = generate_statement_name
      defer1 = connection._prepare(name, sql, datatypes)
      defer1.callback { deferrable.succeed(new(connection, sql, name)) }
      defer1.errback { |errstr| deferrable.fail(errstr) }
      deferrable
    end

    def initialize(connection, sql, name)
      super()
      @connection = connection
      @sql = sql
      @name = name
    end

    #
    # Initiates the bind step for the +Statement+ and returns a +Deferrable+
    #
    # Upon successful completion, the +Deferrable+'s +#callback+ is called with
    # no arguments.
    #
    # @param [Array<Object>] bindvars The values being bound
    # @return [EventMachine::Deferrable]
    #
    def bind(*bindvars)
      super
    end

    #
    # Initiates the execution step for the +Statement+ and returns a
    # +Deferrable+
    #
    # If the +Statement+ was not previously bound, or if +bindvars+ are given,
    # then the method calls +#bind+ and waits for completion before stating the
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

    def self.generate_statement_name
      "stmt_#{rand(1<<32).to_s(16)}"
    end

    def generate_portal_name
      "port_#{name[4..-1]}_#{rand(1<<16).to_s(16)}"
    end

  end

end
