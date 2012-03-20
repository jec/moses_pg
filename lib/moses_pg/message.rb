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

require 'set'
require 'singleton'
require 'stringio'
require 'moses_pg/datatype'
require 'moses_pg/error'

class Integer
  Max_16_Bit = 2**16 - 1
  Mid_16_Bit = 2**15
  Max_32_Bit = 2**32 - 1
  Mid_32_Bit = 2**31
  def to_s16
    (self >= Mid_16_Bit) ? -((self ^ Max_16_Bit) + 1) : self
  end
  def to_s32
    (self >= Mid_32_Bit) ? -((self ^ Max_32_Bit) + 1) : self
  end
end

class String
  def decamelize
    gsub(/[A-Z]/) {|m| $` == '' ? m.downcase : "_#{m.downcase}"}
  end
end

class StringIO
  def read_exactly(n)
    str = read(n)
    raise EOFError, 'End of file reached' if str.nil?
    raise IOError, 'Data was truncated' if str.size < n
    str
  end
  def read_to(delimiter = 0)
    buf = []
    while (ch = self.getbyte) && (ch != delimiter)
      buf << ch
    end
    raise IOError, 'Data was truncated' if ch.nil?
    buf.pack('c*')
  end
end

module MosesPG
  module Message

    Protocol_Version = 3 << 16

    def self.create(code, stream)
      Base.create(code, stream)
    end

    class Base

      @@types = {}

      def self.register
        if @@types.has_key?(self::Code)
          raise MosesPG::Error, "Message code #{self::Code} already registered by #{@@types[self::Code]}"
        end
        @@types[self::Code] = self
      end

      def self.types
        @@types
      end

      def self.create(code, stream)
        if klass = @@types[code]
          klass._create(stream)
        else
          raise ArgumentError, "Message type code #{code.inspect} not supported"
        end
      end

      def self._create(stream)
        allocate.parse(stream)
      end

      def self.event
        name[/::([^:]+)$/, 1].decamelize.to_sym
      end

      def event
        self.class.event
      end

      def parse(stream)
        self
      end

      def dump
        str = _dump
        "#{self.class::Code}#{[str.size + 4].pack('N')}#{str}".force_encoding('binary')
      end

      def _dump
        ''
      end

      def to_s
        ivars = instance_variables.collect { |i| "#{i.to_s[1..-1]}=#{instance_variable_get(i).inspect}" }.join(', ')
        "#<#{self.class.name} #{ivars}>"
      end

    end

    class Authentication < Base

      Code = 'R'

      register

      @@auth_types = {}

      def self.register
        if @@types.has_key?(self::Auth_Type)
          raise MosesPG::Error, "Authorization type #{self::Auth_Type} already registered by #{@@types[self::Auth_Type]}"
        end
        @@auth_types[self::Auth_Type] = self
      end

      def self.auth_types
        @@auth_types
      end

      def self._create(stream)
        auth_type = stream[0..3].unpack('N').first
        if klass = @@auth_types[auth_type]
          klass.allocate.parse(stream)
        else
          raise ArgumentError, "Authorization type #{auth_type.inspect} not supported"
        end
      end

    end

    class AuthenticationOk < Authentication
      Auth_Type = 0
      register
    end

    class AuthenticationKerberosV5 < Authentication
      Auth_Type = 2
      register
    end

    class AuthenticationCleartextPassword < Authentication
      Auth_Type = 3
      register
    end

    class AuthenticationMd5Password < Authentication
      Auth_Type = 5
      register
      attr_reader :salt

      def parse(stream)
        @salt = stream[4..7]
        self
      end
    end

    class AuthenticationScmCredential < Authentication
      Auth_Type = 6
      register
    end

    class AuthenticationGss < Authentication
      Auth_Type = 7
      register
    end

    class AuthenticationSspi < Authentication
      Auth_Type = 9
      register
    end

    class AuthenticationGssContinue < Authentication
      Auth_Type = 8
      register
      attr_reader :auth_data

      def parse(stream)
        @auth_data = stream[4..-1]
        self
      end
    end

    class BackendKeyData < Base
      Code = 'K'
      register
      attr_reader :process_id, :secret_key

      def parse(stream)
        @process_id, @secret_key = stream.unpack('NN')
        self
      end
    end

    #
    # Represents the bind message to send to the server
    #
    class Bind < Base
      Code = 'B'
      Valid_Formats = [0, 1].to_set
      attr_reader :statement_name, :portal_name

      def initialize(statement_name, portal_name, values)
        @statement_name = statement_name
        @portal_name = portal_name
        @param_count = values.size
        @values = values
        @format = values.collect { |value| value.kind_of?(Datatype::Base) ? value.format_code : 0 }
        @result_format = []
      end

      def _dump
        buf = [@portal_name, "\0", @statement_name, "\0", ([@format.size] + @format + [@values.size]).pack('n*')]
        @values.each do |value|
          case value
            when nil
              nil
              buf << [-1].pack('N')
            when Datatype::Base
              str = value.dump
              buf << [str.size].pack('N') << str
            else
              str = value.to_s
              buf << [str.size].pack('N') << str
          end
        end
        buf << ([@result_format.size] + @result_format).pack('n*')
        buf.join
      end
    end

    class BindComplete < Base
      Code = '2'
      register
    end

    class CancelRequest < Base
      Code = ''
      attr_reader :process_id, :secret_key

      def initialize(process_id, secret_key)
        @process_id = process_id
        @secret_key = secret_key
      end

      def dump
        [16, 80877102, @process_id, @secret_key].pack('N*')
      end
    end

    class Close < Base
      Code = 'C'
      attr_reader :name

      def initialize(name)
        @name = name
      end

      def _dump
        "#{self.class::Object_Code}#{@name}\0"
      end
    end

    class ClosePortal < Close
      Object_Code = 'P'
    end

    class CloseStatement < Close
      Object_Code = 'S'
    end

    class CloseComplete < Base
      Code = '3'
      register
    end

    class CommandComplete < Base
      Code = 'C'
      register
      attr_reader :tag

      def parse(stream)
        @tag = stream.strip
        self
      end
    end

    class DataRow < Base
      Code = 'D'
      register
      attr_reader :row

      def parse(stream)
        sio = StringIO.new(stream)
        ncols = sio.read_exactly(2).unpack('n').first
        @row = []
        ncols.times do
          len = sio.read_exactly(4).unpack('N').first.to_s32
          @row << ((len == -1) ? nil : sio.read_exactly(len))
        end
        self
      ensure
        sio.close
      end
    end

    class Describe < Base
      Code = 'D'
      attr_reader :name

      def initialize(name)
        @name = name
      end

      def _dump
        "#{self.class::Object_Code}#{@name}\0"
      end
    end

    class DescribePortal < Describe
      Object_Code = 'P'
    end

    class DescribePrepared < Describe
      Object_Code = 'S'
    end

    class EmptyQueryResponse < Base
      Code = 'I'
      register
    end

    class Execute < Base
      Code = 'E'
      attr_reader :name, :batch_size

      def initialize(name, batch_size = 0)
        @name = name
        @batch_size = batch_size
      end

      def _dump
        "#{@name}\0#{[@batch_size].pack('N')}"
      end
    end

    class ErrorResponse < Base
      Code = 'E'
      register
      attr_reader :errors

      def parse(stream)
        @errors = Hash[stream.scan(/(.)(.*?)\0/)]
        self
      end
    end

    class Flush < Base
      Code = 'H'
      include Singleton
    end

    class NoData < Base
      Code = 'n'
      register
    end

    class NoticeResponse < Base
      Code = 'N'
      register
      Fields = {'S' => 'Severity', 'C' => 'Code', 'M' => 'Message', 'D' => 'Detail', 'H' => 'Hint', 'P' => 'Position',
          'p' => 'Internal position', 'q' => 'Internal query', 'W' => 'Where', 'F' => 'File', 'L' => 'Line', 'R' => 'Routine'}
      attr_reader :fields
      def parse(stream)
        sio = StringIO.new(stream)
        @fields = {}
        begin
          field = sio.read_exactly(1)
          value = sio.read_to(0)
          @fields[Fields[field] || field] = value
        rescue IOError
          break
        end until false
        self
      ensure
        sio.close
      end
    end

    class ParameterDescription < Base
      Code = 't'
      register
      attr_reader :oids

      def parse(stream)
        sio = StringIO.new(stream)
        ncols = sio.read_exactly(2).unpack('n').first
        @oids = []
        ncols.times { @oids << sio.read_exactly(4).unpack('N').first }
        self
      ensure
        sio.close
      end
    end

    class ParameterStatus < Base
      Code = 'S'
      register
      attr_reader :name, :value

      def parse(stream)
        @name, @value = stream.scan(/(.*?)\0/).collect { |x| x.first }
        raise IOError, 'Data was truncated' if @name.nil? || @value.nil?
        self
      end
    end

    class Parse < Base
      Code = 'P'
      attr_reader :name, :sql, :datatypes

      def initialize(name, sql, datatypes)
        @name = name
        @sql = sql
        @datatypes = datatypes.nil? ? [] : datatypes
      end

      def _dump
        [@name, "\0", @sql, "\0", [@datatypes.size].pack('n'), @datatypes.pack('N*')].join
      end
    end

    class ParseComplete < Base
      Code = '1'
      register
    end

    class PasswordMessage < Base
      Code = 'p'
      attr_reader :password

      def initialize(password)
        @password = password
      end

      def _dump
        "#{@password}\0"
      end
    end

    class PortalSuspended < Base
      Code = 's'
      register
    end

    class Query < Base
      Code = 'Q'
      attr_reader :sql

      def initialize(sql)
        @sql = sql
      end

      def _dump
        "#{@sql}\0"
      end
    end

    class ReadyForQuery < Base
      Code = 'Z'
      register
      attr_reader :status

      def parse(stream)
        @status = stream[0] || raise(IOError, 'Data was truncated')
        self
      end
    end

    class RowDescription < Base
      Code = 'T'
      register
      attr_reader :columns

      def parse(stream)
        sio = StringIO.new(stream)
        ncolumns = sio.read_exactly(2).unpack('n').first
        @columns = Array.new(ncolumns)
        ncolumns.times do |i|
          @columns[i] = [
            sio.read_to,
            sio.read_exactly(4).unpack('N').first.to_s32,
            sio.read_exactly(2).unpack('n').first.to_s16,
            sio.read_exactly(4).unpack('N').first.to_s32,
            sio.read_exactly(2).unpack('n').first.to_s16,
            sio.read_exactly(4).unpack('N').first.to_s32,
            sio.read_exactly(2).unpack('n').first.to_s16
          ]
        end
        self
      ensure
        sio.close
      end
    end

    class StartupMessage < Base
      Code = ''
      def initialize(user, dbname = nil)
        @user = user
        @dbname = dbname
      end
      def _dump
        str = "#{[Protocol_Version].pack('N')}user\0#{@user}\0"
        str += "database\0#{@dbname}\0" if @dbname
        str += "\0"
        str
      end
    end

    class Sync < Base
      Code = 'S'
      include Singleton
    end

    class Terminate < Base
      Code = 'X'
      include Singleton
    end

  end
end
