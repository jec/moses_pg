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

require 'time'
require 'date'
require 'moses_pg/error'

module MosesPG
  module Datatype

    def self.types
      Base.types
    end

    class Base

      @@types = {}

      #
      # Registers a subclass for an _oid_, with fixed _precision_ and _scale_
      #
      # If the datatype's precision or scale depends on the value of the type
      # mod, then specify +nil+ for that parameter, and provide a
      # +::decode_mod+ method to return the proper precision and scale based on
      # the type mod.
      #
      # @raise [RuntimeError] If the _oid_ has already been registered
      # @param [Integer] oid The OID to associate with the calling class
      # @param [Integer] precision The default precision to associate with the calling class
      # @param [Integer] scale The default scale to associate with the calling class
      # @return [Class] self
      #
      def self.register(oid, precision = nil, scale = nil)
        raise MosesPG::Error, "OID #{oid} already registered" if @@types.has_key?(oid)
        register!(oid, precision, scale)
        self
      end

      #
      # Registers a subclass for an _oid_; does not raise an exception for
      # overriding a previously registered class
      #
      # This one does *not* raise an error if _oid_ has already been registered,
      # which allows a user to configure a custom class for type translation.
      #
      # @param (see .register)
      # @return [Class] self
      #
      def self.register!(oid, precision = nil, scale = nil)
        @@types[oid] = self
        self.const_set(:OID, oid)
        self.const_set(:Precision, precision)
        self.const_set(:Scale, scale)
        self
      end

      # @return [Hash]
      def self.types
        @@types
      end

      # @return [Class]
      def self.class_for(oid, mod)
        if klass = @@types[oid]
          name = klass.name_for(mod)
          if Datatype.const_defined?(name)
            Datatype.const_get(name)
          else
            new_klass = Class.new(klass)
            Datatype.const_set(name, new_klass)
            new_klass
          end
        else
          STDERR.puts "OID #{oid.inspect} not recognized"
          Unknown
        end
      end

      # @param[Integer] mod The type mod for which to generate a class name
      # @return [Symbol]
      def self.name_for(mod)
        a = ([self.name[/::([^:]+)$/, 1]] + decode_mod(mod))
        a.compact!
        a.join('_').to_sym
      end

      # @param[Integer] mod The type mod to decode
      # @return [Array<Integer, Integer>]
      def self.decode_mod(mod)
        [nil, nil]
      end

      # @param[Object] obj The object to translate
      # @return [Object]
      def self.translate(obj)
        obj
      end

      # @param[Object] value The initial value
      # @return [MosesDB::Datatype::Base]
      def initialize(value)
        @value = value
      end

      # @return [Integer, nil]
      def precision
        self.class::Precision
      end

      # @return [Integer, nil]
      def scale
        self.class::Scale
      end

      # @return [Integer]
      def oid
        self.class::OID
      end

      #
      # Returns the format code (0 for text or 1 for binary) to use for binding
      # an object of this type to a parameterized query
      #
      # @return [Integer]
      def format_code
        0
      end

      #
      # Returns the format code (0 for text or 1 for binary) to use for
      # outputting an object of this type from a query
      #
      # @return [Integer]
      def self.result_format_code
        0
      end

      # @return [String]
      def dump
        @value.to_s
      end

      # @return [String]
      def to_s
        "#<#{self.class.name} precision=#{precision.inspect} scale=#{scale.inspect} value=#{@value.inspect}>"
      end

    end

    class Bigint < Base
      register(20, 19, 0)
      def self.translate(obj)
        obj.to_i
      end
    end

    class Bit < Base
      register(1560)
      def self.decode_mod(mod)
        [mod, nil]
      end
    end

    class Boolean < Base
      register(16)
      def self.translate(obj)
        obj == 't'
      end
    end

    class Box < Base
      register(603)
    end

    class BPChar < Base
      register(1042)
      def self.decode_mod(mod)
        if mod == -1
          [0, nil]
        else
          [mod - 4, nil]
        end
      end
    end

    class Bytea < Base
      register(17)
      def format_code
        1
      end
      def self.result_format_code
        1
      end
      def dump
        @value.to_s.force_encoding('binary')
      end
    end

    class Char < Base
      register(18, 1)
    end

    class CIDR < Base
      register(650)
    end

    class Circle < Base
      register(718)
    end

    class Date < Base
      register(1082)
      def self.translate(obj)
        ::Date.parse(obj)
      end
    end

    class Float4 < Base
      register(700, 6, 6)
      def self.translate(obj)
        obj.to_f
      end
    end

    class Float8 < Base
      register(701, 15, 15)
      def self.translate(obj)
        obj.to_f
      end
    end

    class Inet < Base
      register(869)
    end

    class Integer < Base
      register(23, 10, 0)
      def self.translate(obj)
        obj.to_i
      end
    end

    class Interval < Base
      register(1186)
      def self.decode_mod(mod)
        [(mod == -1) ? 6 : mod, nil]
      end
    end

    class LSeg < Base
      register(601)
    end

    class MACAddr < Base
      register(829)
    end

    class Money < Base
      register(790)
    end

    class Name < Base
      register(19)
    end

    class Numeric < Base
      register(1700)
      def self.decode_mod(mod)
        if mod == -1
          [nil, nil]
        else
          [((mod - 4) & 0xFFFF0000) >> 16, (mod - 4) & 0xFFFF]
        end
      end
      def self.translate(obj)
        (self::Scale != 0) ? obj.to_f : obj.to_i
      end
    end

    class Oid < Base
      register(26, 10, 0)
    end

    class Path < Base
      register(602)
    end

    class Point < Base
      register(600)
    end

    class Polygon < Base
      register(604)
    end

    class Smallint < Base
      register(21, 5, 0)
      def self.translate(obj)
        obj.to_i
      end
    end

    class Text < Base
      register(25)
    end

    class Time < Base
      register(1083)
      def self.decode_mod(mod)
        [(mod == -1) ? 6 : mod, nil]
      end
    end

    class Timestamp < Base
      register(1114)
      def self.decode_mod(mod)
        [(mod == -1) ? 6 : mod, nil]
      end
      def self.translate(obj)
        ::Time.parse(obj)
      end
    end

    class TimestampTZ < Base
      register(1184)
      def self.decode_mod(mod)
        [(mod == -1) ? 6 : mod, nil]
      end
      def self.translate(obj)
        ::Time.parse(obj)
      end
    end

    class TimeTZ < Base
      register(1266)
      def self.decode_mod(mod)
        [(mod == -1) ? 6 : mod, nil]
      end
    end

    class TSQuery < Base
      register(3615)
    end

    class TSVector < Base
      register(3614)
    end

    class Unknown < Base
      register(705)
    end

    class UUID < Base
      register(2950)
    end

    class Varbit < Base
      register(1562)
      def self.decode_mod(mod)
        [(mod == -1) ? 0 : mod, nil]
      end
    end

    class Varchar < Base
      register(1043)
      def self.decode_mod(mod)
        [(mod == -1) ? 0 : mod - 4, nil]
      end
    end

    class Void < Base
      register(2278)
      def self.translate(obj)
        nil
      end
    end

    class XML < Base
      register(142)
    end

  end
end
