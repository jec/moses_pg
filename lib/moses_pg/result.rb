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

require 'moses_pg/column'

module MosesPG

  #
  # Encapsulates the result of a single SQL command
  #
  # Once all the results are collected from the server, the +Connection+ calls
  # the +#result+ method, which simply returns +self+. This is to parallel the
  # functionality of +ResultGroup#result+.
  #
  class Result

    # Returns the Array of +Column+s
    # @return [Array<MosesPG::Column>]
    attr_accessor :columns

    # Returns the Array of parameter +Datatype+ classes
    # @return [Array<Class>]
    attr_reader :parameters

    # Returns the +Connection+ that created this +Result+
    # @return [MosesPG::Connection]
    attr_reader :connection

    # Returns the Array of Arrays of values
    # @return [Array<Array<Object>>]
    attr_reader :rows

    # Returns the Array of Hashes representing any notices received
    # @return [Array<Hash>]
    attr_reader :notices

    # Returns the tag that summarizes the action that was taken
    # @return [String]
    attr_reader :tag

    # Returns the number of rows processed, if available
    # @return [Integer]
    attr_reader :processed_row_count

    def initialize(connection)
      @connection = connection
      @rows = []
      @notices = []
    end

    #
    # Called by the +Connection+ to set the list of +Column+s when the
    # information is received from the server
    #
    # @param [Array<Array<Object>>] cols The raw data values that describe the
    #   columns
    #
    def set_raw_columns(cols)
      @columns = cols.collect { |args| Column.new(*args) }
    end

    # @param [Array<Integer>] oids The OIDs of the required parameters
    def set_raw_parameters(oids)
      @parameters = oids.collect { |oid| Datatype::Base.class_for(oid, -1) }
    end

    #
    # Adds a data row to the +Result+
    #
    # @param [Array<Object>] row A row of column values
    # @return [MosesPG::Result]
    #
    def <<(row)
      @rows << row
      self
    end

    #
    # Adds a notice Hash to the +Result+
    #
    # @param [Hash] notice A Hash with the notice content
    # @return [MosesPG::Result]
    #
    def add_notice(notice)
      @notices << notice
      self
    end

    #
    # Yields the rows returned by the server, without type translation
    #
    # This method performs no magic; it's equivalent to +#rows.each+. It's just
    # here for symmetry with +#each_row_as_native+.
    #
    # @yieldparam [Array<Object>] row
    # @return [MosesPG::Result]
    #
    def each_row
      @rows.each { |row| yield(row) }
      self
    end

    #
    # Yields the rows with type translation
    #
    # This method is preferred over +#rows_as_native+ for large result sets,
    # since it doesn't save the translated data in the +Result+ object.
    #
    # @yieldparam [Array<Object>] row
    # @return [MosesPG::Result]
    #
    def each_row_as_native
      @rows.each { |row| yield(Array.new(row.size) { |i| @columns[i].type.translate(row[i]) }) }
      self
    end

    #
    # Returns the rows with type translation
    #
    # This method saves the translated data in the +Result+ object, in case it
    # is called again. For this reason, you may consider using
    # +#each_row_as_native+ for large result sets.
    #
    # @return [Array<Array<Object>>]
    #
    def rows_as_native
      unless @rows_as_native
        @rows_as_native = @rows.collect { |row| Array.new(row.size) { |i| @columns[i].type.translate(row[i]) } }
      end
      @rows_as_native
    end

    #
    # Called by the +Connection+ to mark the +Result+ as complete and set the
    # _tag_
    #
    # @param [String] tag The tag that summarizes the action taken
    # @return [MosesPG::Result]
    #
    def finish(tag)
      @tag = tag
      if nrows = tag[/\s(\d+)$/, 1]
        @processed_row_count = nrows.to_i
      end
      self
    end

    #
    # Indicates whether the +Result+ has been marked as completed
    #
    def finished?
      !!@tag
    end

    #
    # Called by the +Connection+ upon completion of a SQL command, or a series of
    # commands
    #
    # @return [MosesPG::Result]
    #
    def result
      self
    end

    #
    # @return [String]
    #
    def to_s
      "#<#{self.class.name} columns=#{@columns.inspect} rows=#{@rows.inspect} tag=#{@tag.inspect}>"
    end

  end

  #
  # Collects the results of multiple SQL commands
  #
  # An object of this class is used when +Connection#execute+ is called, since
  # it can be used to pass multiple SQL commands to the server.
  #
  # Once all the results are collected from the server, the +Connection+ calls
  # the +#result+ method, which returns an array of +Result+s, one for each SQL
  # command.
  #
  class ResultGroup

    attr_reader :result

    def initialize(connection)
      @connection = connection
      @result = [Result.new(connection)]
    end

    def set_raw_columns(cols)
      current_result { |res| res.set_raw_columns(cols) }
      self
    end

    def set_raw_parameters(oids)
      current_result { |res| res.set_raw_parameters(oids) }
      self
    end

    def columns
      current_result { |res| res.columns }
    end

    def <<(row)
      current_result { |res| res << row }
      self
    end

    def add_notice(notice)
      current_result { |res| res.add_notice(notice) }
      self
    end

    def finish(tag)
      current_result { |res| res.finish(tag) }
      self
    end

    def current_result
      if @result.last.finished?
        res = Result.new(@connection)
        @result << res
        yield(res)
      else
        yield(@result.last)
      end
    end

  end

end
