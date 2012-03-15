# encoding: utf-8

require 'time'
require 'date'

module MosesPG
  module Datatype

    def self.types
      Base.types
    end

    class Base

      @@types = {}

      private_class_method :new

      def self.register(oid)
        raise "OID #{oid} already registered" if @@types.has_key?(oid)
        @@types[oid] = self
        self.const_set(:OID, oid)
      end

      def self.types
        @@types
      end

      def self.create(oid, mod)
        if klass = @@types[oid]
          klass.class_eval { new(mod) }
        else
          STDERR.puts "OID #{oid.inspect} not recognized"
          Unknown.class_eval { new(mod) }
        end
      end

      def initialize(mod)
        @mod = mod
      end

      def translate(obj)
        obj
      end

      def precision
        0
      end

      def scale
        0
      end

      def oid
        self.class::OID
      end

      def to_s
        ivars = []
        [:@precision, :@scale].each do |ivar|
          ivars << "#{ivar.to_s[1..-1]}=#{instance_variable_get(ivar).inspect}" if instance_variable_defined?(ivar)
        end
        "#<#{self.class.name} #{ivars.join(', ')}>"
      end

    end

    class Bigint < Base
      register(20)
      def translate(obj)
        obj.to_i
      end
      def precision
        19
      end
    end

    class Bit < Base
      register(1560)
      def precision
        @mod
      end
    end

    class Boolean < Base
      register(16)
      def translate(obj)
        obj == 't'
      end
      def precision
        1
      end
    end

    class Box < Base
      register(603)
    end

    class BPChar < Base
      register(1042)
      attr_reader :precision
      def initialize(mod)
        super
        if @mod == -1
          @precision = 0
        else
          @precision = @mod - 4
        end
      end
    end

    class Bytea < Base
      register(17)
      # assumes _obj_ is a String with the prefix \x followed by hex
      def translate(obj)
        obj[2..-1].scan(/../).collect { |x| x.to_i(16) }.pack('c*')
      end
    end

    class Char < Base
      register(18)
      def precision
        1
      end
    end

    class CIDR < Base
      register(650)
    end

    class Circle < Base
      register(718)
    end

    class Date < Base
      register(1082)
      def translate(obj)
        ::Date.parse(obj)
      end
    end

    class Float4 < Base
      register(700)
      def translate(obj)
        obj.to_f
      end
      def precision
        6
      end
      def scale
        6
      end
    end

    class Float8 < Base
      register(701)
      def translate(obj)
        obj.to_f
      end
      def precision
        15
      end
      def scale
        15
      end
    end

    class Inet < Base
      register(869)
    end

    class Integer < Base
      register(23)
      def translate(obj)
        obj.to_i
      end
      def precision
        10
      end
    end

    class Interval < Base
      register(1186)
      attr_reader :precision
      def initialize(mod)
        super
        @precision = (@mod == -1) ? 6 : @mod
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
      attr_reader :precision, :scale
      def initialize(mod)
        super
        if @mod == -1
          @precision = @scale = nil
        else
          @precision = ((@mod - 4) & 0xFFFF0000) >> 16
          @scale = (@mod - 4) & 0xFFFF
        end
      end
      def translate(obj)
        (@scale != 0) ? obj.to_f : obj.to_i
      end
    end

    class Oid < Base
      register(26)
      def precision
        10
      end
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
      register(21)
      def translate(obj)
        obj.to_i
      end
      def precision
        5
      end
    end

    class Text < Base
      register(25)
    end

    class Time < Base
      register(1083)
      attr_reader :precision
      def initialize(mod)
        super
        @precision = (@mod == -1) ? 6 : @mod
      end
    end

    class Timestamp < Base
      register(1114)
      attr_reader :precision
      def initialize(mod)
        super
        @precision = (@mod == -1) ? 6 : @mod
      end
      def translate(obj)
        ::Time.parse(obj)
      end
    end

    class TimestampTZ < Base
      register(1184)
      attr_reader :precision
      def initialize(mod)
        super
        @precision = (@mod == -1) ? 6 : @mod
      end
      def translate(obj)
        ::Time.parse(obj)
      end
    end

    class TimeTZ < Base
      register(1266)
      attr_reader :precision
      def initialize(mod)
        super
        @precision = (@mod == -1) ? 6 : @mod
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
      attr_reader :precision
      def initialize(mod)
        super
        @precision = (@mod == -1) ? 0 : @mod
      end
    end

    class Varchar < Base
      register(1043)
      attr_reader :precision
      def initialize(mod)
        super
        if @mod == -1
          @precision = 0
        else
          @precision = @mod - 4
        end
      end
    end

    class Void < Base
      register(2278)
    end

    class XML < Base
      register(142)
    end

  end
end
