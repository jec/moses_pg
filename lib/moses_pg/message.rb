# encoding: utf-8

module MosesPG
  module Message

    Protocol_Version = 3 << 16

    def self.create(code, stream)
      Base.create(code, stream)
    end

    class Base

      @@types = {}

      def self.register
        #puts "Registering #{self.name} with code #{self::Code}"
        raise "Message code #{self::Code} already registered by #{@@types[self::Code]}" if @@types.has_key?(self::Code)
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
        obj = allocate
        obj.parse(stream)
      end

    end

    class Authentication < Base

      Code = 'R'

      register

      @@auth_types = {}

      def self.register
        #puts "Registering #{self.name} with auth type #{self::Auth_Type}"
        if @@types.has_key?(self::Auth_Type)
          raise "Authorization type #{self::Auth_Type} already registered by #{@@types[self::Auth_Type]}"
        end
        @@auth_types[self::Auth_Type] = self
      end

      def self.auth_types
        @@auth_types
      end

      def self._create(stream)
        auth_type = stream[0..3].unpack('N').first
        if klass = @@auth_types[auth_type]
          klass.allocate
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

    class AuthenticationMD5Password < Authentication

      Auth_Type = 5

      register

    end

    class AuthenticationSCMCredential < Authentication

      Auth_Type = 6

      register

    end

    class AuthenticationGSS < Authentication

      Auth_Type = 7

      register

    end

    class AuthenticationSSPI < Authentication

      Auth_Type = 9

      register

    end

    class AuthenticationGSSContinue < Authentication

      Auth_Type = 8

      register

    end

    class ErrorResponse < Base

      Code = 'E'

      register

      def parse(stream)
        @errors = Hash[stream.scan(/(.)(.*?)\0/)]
      end

    end

    class StartupMessage < Base

      Code = '' # This one has no type code

      def initialize(user, dbname = nil)
        @user = user
        @dbname = dbname
      end

      def dump
        str = "#{[Protocol_Version].pack('N')}user\0#{@user}\0"
        str += "database\0#{@dbname}\0" if @dbname
        str += "\0"
        [str.size + 4].pack('N') + str
      end

    end

  end
end
