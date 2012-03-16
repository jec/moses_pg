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
    # @return [Connection]
    #
    def self.connect!(opts = {})
      EM::Synchrony.sync(connect(opts))
    end

    #
    # Submits SQL command (or commands) to the PostgreSQL server, blocking
    # until completed; returns an Array of +Result+s
    #
    # @param [String] sql A single SQL command or multiple commands, separated
    #   by semicolons
    # @return [Array<Result>]
    #
    def execute!(sql)
      EM::Synchrony.sync(execute(sql))
    end

    #
    # Submits a single SQL command for parsing, blocking until completed;
    # returns the +Connection+
    #
    # @param [String] name A name used to refer to the SQL later for execution
    # @param [String] sql A single SQL command to be parsed and saved
    # @param [nil, Array<Integer>] datatypes The data type(s) to use for the parameters
    #   included in the SQL command, if any
    # @return [Connection]
    #
    def prepare!(name, sql, datatypes = nil)
      EM::Synchrony.sync(prepare(name, sql, datatypes))
      self
    end

    #
    # Initiates the execution of a previously prepared SQL command, blocking
    # until the command completes; returns a +Result+
    #
    # @param [String] name The name given to the SQL command when +#prepare+
    #   was called
    # @param [Array<Object>] bindvars The values to bind to the placeholders in
    #   the SQL command, if any
    # @return [Result]
    #
    def execute_prepared!(name, *bindvars)
      EM::Synchrony.sync(execute_prepared(name, *bindvars))
    end

  end

end
