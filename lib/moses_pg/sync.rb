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

require 'moses_pg'
require 'em-synchrony'

module MosesPG

  #
  # Convenience method to access +Connection::connect!+
  #
  # @return [Connection]
  #
  def self.connect!(opts = {})
    Connection.connect!(opts)
  end

  class Connection

    #
    # Creates a connection to PostgreSQL, blocking until the connecting and
    # authenticating are completed; returns the +Connection+
    #
    # @option opts [String] :host The hostname or IP address of the PostgreSQL
    #   server (if not specified, connects via the UNIX domain socket)
    # @option opts [Integer] :port The port (defaults to 5432)
    # @option opts [String] :dbname The database name
    # @option opts [String] :user The user name
    # @option opts [String] :password The password
    # @option opts [#trace, #debug, #info, #warn, #error, #fatal] :logger A logging object
    # @raise [MosesPG::Error]
    # @return [MosesPG::Connection]
    #
    def self.connect!(opts = {})
      conn = EM::Synchrony.sync(connect(opts))
      raise conn if conn.kind_of?(Error)
      conn
    end

    #
    # Submits SQL command (or commands) to the PostgreSQL server, blocking
    # until completed; returns an Array of +Result+s
    #
    # @param [String] sql A single SQL command or multiple commands, separated
    #   by semicolons
    # @raise [MosesPG::Error]
    # @return [Array<MosesPG::Result>]
    #
    def execute!(sql)
      result = EM::Synchrony.sync(execute(sql))
      raise result if result.kind_of?(Error)
      result
    end

    #
    # Submits a single SQL command for parsing, blocking until completed;
    # returns the +Connection+
    #
    # @param [String] name A name used to refer to the SQL later for execution
    # @param [String] sql A single SQL command to be parsed and saved
    # @param [nil, Array<Integer>] datatypes The data type(s) to use for the parameters
    #   included in the SQL command, if any
    # @raise [MosesPG::Error]
    # @return [MosesPG::Connection]
    #
    def prepare!(sql, datatypes = nil)
      Statement.prepare!(self, sql, datatypes)
    end

    def transaction!
      start_transaction!
      if block_given?
        begin
          result = yield
        rescue Exception => e
          rollback!
          raise
        else
          commit!
          result
        end
      else
        self
      end
    end

    def start_transaction!
      result = EM::Synchrony.sync(_start_transaction)
      raise result if result.kind_of?(Error)
      self
    end

    def commit!
      result = EM::Synchrony.sync(commit)
      raise result if result.kind_of?(Error)
      self
    end

    def rollback!
      result = EM::Synchrony.sync(rollback)
      raise result if result.kind_of?(Error)
      self
    end

  end

  class Statement

    #
    # Submits a SQL command for parsing, blocking until completed; returns the
    # +Statement+
    #
    # @param [MosesPG::Connection] connection The +Connection+ in which to prepare the SQL
    # @param [String] sql The SQL text to prepare
    # @param [Array<Integer>] datatypes 0 for text and 1 for binary
    # @raise [MosesPG::Error]
    # @return [MosesPG::Connection]
    #
    def self.prepare!(connection, sql, datatypes = nil)
      result = EM::Synchrony.sync(prepare(connection, sql, datatypes))
      raise result if result.kind_of?(Error)
      result
    end

    #
    # Initiates the bind step for the +Statement+, blocking until completed;
    # returns the +Statement+
    #
    # @param [Array<Object>] bindvars The values being bound
    # @raise [MosesPG::Error]
    # @return [MosesPG::Statement]
    #
    def bind!(*bindvars)
      result = EM::Synchrony.sync(bind(*bindvars))
      raise result if result.kind_of?(Error)
      self
    end

    #
    # Initiates the execution step for the +Statement+, blocking until
    # completed; returns the +Result+
    #
    # If the +Statement+ was not previously bound, or if +bindvars+ are given,
    # then the method calls +#bind!+ and waits for completion before stating
    # the execution.
    #
    # @param [Array<Object>] bindvars The values being bound
    # @raise [MosesPG::Error]
    # @return [MosesPG::Result]
    #
    def execute!(*bindvars)
      result = EM::Synchrony.sync(execute(*bindvars))
      raise result if result.kind_of?(Error)
      result
    end

    #
    # Initiates the closing of the +Statement+, blocking until completed;
    # returns the +Statement+
    #
    # @raise [MosesPG::Error]
    # @return [MosesPG::Statement]
    #
    def close!
      result = EM::Synchrony.sync(close)
      raise result if result.kind_of?(Error)
      self
    end

  end

end
