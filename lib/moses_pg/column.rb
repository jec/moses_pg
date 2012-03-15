# encoding: utf-8

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
      "#<#{self.class.name} name=#{@name.inspect} type=#{@type} format=#{@format.inspect}>"
    end

  end

end
