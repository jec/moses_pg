# encoding: utf-8

# MosesPG -- a Ruby library for accessing PostgreSQL
# Copyright (C) 2012 James Edwin Cain (user: moses_pg; domain: jcain.net)
# 
# This file is part of the MosesPG library.  This Library is free software; you
# may redistribute it or modify it under the terms of the license contained in
# the file LICENCE.txt. If you did not receive a copy of the license, please
# contact the copyright holder.

require 'moses_pg/datatype'

module MosesPG

  class Column

    attr_reader :name, :type, :table_oid, :table_attr_num, :oid, :type_length, :mod, :format

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

    def text?
      @format == 0
    end

    def binary?
      @format == 1
    end

    def to_s
      "#<#{self.class.name} name=#{@name.inspect}, type=#{@type}, format=#{@format.inspect}>"
    end

  end

end
