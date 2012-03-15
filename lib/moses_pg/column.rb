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

require 'moses_pg/datatype'

module MosesPG

  #
  # Contains metadata for a column
  #
  class Column

    # @return [String] The column name
    attr_reader :name

    # @return [Datatype] The +Datatype+ object used for translation
    attr_reader :type

    # @return [Integer] The OID of the table, if any
    attr_reader :table_oid

    # @return [Integer] The attribute number of the column in the table, if any
    attr_reader :table_attr_num

    # @return [Integer] The OID of the column value
    attr_reader :oid

    # @return [Integer] The length of the value's data type, if a fixed size
    attr_reader :type_length

    # @return [Integer] The type mod of the column value
    attr_reader :mod

    # @return [Integer] 0 for text or 1 for binary
    attr_reader :format

    #
    # Creates a new +Column+ from the raw metadata returned by the server
    #
    # @param [String] name The column name
    # @param [Integer] table_oid The OID of the table, if any
    # @param [Integer] table_attr_num The attribute number of the column in the
    #   table, if any
    # @param [Integer] oid The OID of the column value
    # @param [Integer] type_length The length of the value's data type, if a
    #   fixed size
    # @param [Integer] mod The type mod of the column value
    # @param [Integer] format 0 for text or 1 for binary
    #
    def initialize(name, table_oid, table_attr_num, oid, type_length, mod, format)
      @name = name
      @type = Datatype::Base.create(oid, mod)
      @table_oid = table_oid
      @table_attr_num = table_attr_num
      @oid = oid
      @type_length = type_length
      @mod = mod
      @format = format
    end

    #
    # +true+ if the row data was returned as text
    #
    def text?
      @format == 0
    end

    #
    # +true+ if the row data was returned as binary
    #
    def binary?
      @format == 1
    end

    # @return [String]
    def to_s
      "#<#{self.class.name} name=#{@name.inspect}, type=#{@type}, format=#{@format.inspect}>"
    end

  end

end
