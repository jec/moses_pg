# encoding: utf-8

require 'eventmachine'
require 'etc'
require 'moses_pg/message'
require 'moses_pg/message/buffer'

module MosesPG

  def self.connect(opts = {})
    Connection.connect(opts)
  end

  class Connection < ::EM::Connection

    Default_Host = 'localhost'
    Default_Port = 5432

    def self.connect(opts = {})
      host = opts[:host]
      port = opts[:port] || Default_Port
      if host
        ::EM.connect(host, port, self, opts)
      else
        ::EM.connect_unix_domain("/tmp/.s.PGSQL.#{port}", self, opts)
      end
    end

    def initialize(opts = {})
      super
      @dbname = opts[:dbname]
      @user = opts[:user] || Etc.getlogin
      @password = opts[:password]
      @buffer = MosesPG::Message::Buffer.new
    end

    def post_init
      puts 'in #post_init'
      send_data(MosesPG::Message::StartupMessage.new(@user, @dbname).dump)
    end

    def receive_data(data)
      puts "received data #{data.inspect}"
      @buffer.receive(data) do |message_type_char, raw_message|
        message = MosesPG::Message.create(message_type_char, raw_message)
        p message
      end
    end

    def unbind
      puts 'in #unbind'
    end

  end

end
