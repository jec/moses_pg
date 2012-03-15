# encoding: utf-8

# MosesPG -- a Ruby library for accessing PostgreSQL
# Copyright (C) 2012 James Edwin Cain (user: moses_pg; domain: jcain.net)
# 
# This file is part of the MosesPG library.  This Library is free software; you
# may redistribute it or modify it under the terms of the license contained in
# the file LICENCE.txt. If you did not receive a copy of the license, please
# contact the copyright holder.

require 'moses_pg/column'

module MosesPG

  class Result

    attr_reader :connection, :rows, :columns, :tag

    def initialize(connection)
      @connection = connection
      @rows = []
    end

    def columns=(cols)
      @columns = cols.collect { |args| Column.new(*args) }
    end

    def <<(row)
      @rows << row
      self
    end

    def each_row
      @rows.each { |row| yield(row) }
    end

    def each_row_as_native
      @rows.each { |row| yield(Array.new(row.size) { |i| @columns[i].type.translate(row[i]) }) }
    end

    def finish(tag)
      @tag = tag
      self
    end

    def finished?
      !!@tag
    end

    def result
      self
    end

    def to_s
      "#<#{self.class.name} columns=#{@columns.inspect} rows=#{@rows.inspect} tag=#{@tag.inspect}>"
    end

  end

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
