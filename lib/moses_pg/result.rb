# encoding: utf-8

#--
# MosesPG -- a Ruby library for accessing PostgreSQL
# Copyright (C) 2012 James Edwin Cain (user: moses_pg; domain: jcain.net)
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

    # Returns the +Connection+ that created this +Result+
    # @return [Connection]
    attr_reader :connection

    # Returns the Array of Arrays of values
    # @return [Array<Array<Object>>]
    attr_reader :rows

    # Returns the Array of +Column+s
    # @return [Array<Column>]
    attr_reader :columns

    # Returns the tag that summarizes the action that was taken
    # @return [String]
    attr_reader :tag

    def initialize(connection)
      @connection = connection
      @rows = []
    end

    #
    # Called by the +Connection+ to set the list of +Column+s when the
    # information is received from the server
    #
    # @param [Array<Array<Object>>] cols The raw data values that describe the
    #   columns
    #
    def columns=(cols)
      @columns = cols.collect { |args| Column.new(*args) }
    end

    #
    # Adds a data row to the +Result+
    #
    # @param [Array<Object>] row A row of column values
    # @return [Result]
    #
    def <<(row)
      @rows << row
      self
    end

    #
    # Yields the rows returned by the server, without type translation
    #
    # This method performs no magic; it's equivalent to +#rows.each+. It's just
    # here for symmetry with +#each_row_as_native+.
    #
    # @yieldparam [Array<Object>] row
    # @return [Result]
    #
    def each_row
      @rows.each { |row| yield(row) }
      self
    end

    #
    # Yields the rows with type translation
    #
    # @yieldparam [Array<Object>] row
    # @return [Result]
    #
    def each_row_as_native
      @rows.each { |row| yield(Array.new(row.size) { |i| @columns[i].type.translate(row[i]) }) }
      self
    end

    #
    # Called by the +Connection+ to mark the +Result+ as complete and set the
    # _tag_
    #
    # @param [String] tag The tag that summarizes the action taken
    # @return [Result]
    #
    def finish(tag)
      @tag = tag
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
    # @return [Result]
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

    def columns=(cols)
      current_result { |res| res.columns = cols }
    end

    def <<(cols)
      current_result { |res| res << cols }
      self
    end

    def finish(tag)
      current_result { |res| res.finish(tag) }
      self
    end

    def current_result
      if @result.last.finished?
        res = Result.new(connection)
        @result << res
        yield(res)
      else
        yield(@result.last)
      end
    end

  end

end
