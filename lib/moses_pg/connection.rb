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

require 'etc'
require 'eventmachine'
require 'moses_pg/message'
require 'moses_pg/message/buffer'
require 'moses_pg/result'
require 'state_machine'

#
# MosesPG provides EventMachine-based access to PostgreSQL using version 3.0 of
# their frontend/backend protocol.
#
module MosesPG

  #
  # Convenience method to access +Connection::connect+
  #
  # @return [EventMachine::Deferrable]
  #
  def self.connect(opts = {})
    Connection.connect(opts)
  end

  FakeResult = Struct.new(:result) # @private

  class NullLogger # @private
    def trace(*s) end
    def debug(*s) end
    def info(*s) end
    def warn(*s) end
    def error(*s) end
    def fatal(*s) end
  end

  #
  # Manages a PostgreSQL connection 
  #
  # Each operation that submits data to the server is implemented by two
  # methods: one with a bang (!) and one without.  The bang methods (e.g.
  # +#connect!+, +#execute!+) block until the command is completed and return
  # the appropriate data. The non-bang methods (e.g. +#connect+, +#execute+) do
  # not block and instead return a +Deferrable+ immediately. Data returned from
  # the server is passed through the +Deferrable+'s +#callback+ method.
  #
  # If you intend to call any of the bang methods, then you must start
  # EventMachine's reactor using +EM::synchrony+ instead of +EM::run+. Otherwise,
  # you will get the error <em>"can't yield from root fiber."</em>
  #
  # Since the PostgreSQL frontend/backend protocol messages carry no sequence
  # numbers to identify which results belong to which queries, it is necessary
  # to submit queries to the backend one at a time.  However, +Connection+
  # takes care of this serialization for you.  It is not necessary to wait for
  # a response to +Deferrable+ before submitting additional queries, either
  # with the bang or non-bang methods.  The +Connection+ will queue the
  # requests if necessary and run them in sequence as the PostgreSQL server
  # responds with the previous results.
  #
  class Connection < ::EM::Connection

    Default_Host = 'localhost'
    Default_Port = 5432

    state_machine :initial => :startup do
      # log all transitions
      after_transition any => any do |obj, trans|
        obj.logger.trace { "+++ On event #{trans.event}: #{trans.from_name} => #{trans.to_name}" }
      end
      # entering a failure state fails the query
      after_transition any => :connection_failed, :do => :_fail_connection
      after_transition any => :query_failed, :do => :_fail_query
      after_transition any => :parse_failed, :do => :_fail_parse
      after_transition any => :bind_failed, :do => :_fail_bind
      after_transition any => :execute_failed, :do => :_fail_execute

      # entering the ready state checks the query queue and calls succeed for
      # the previous command
      after_transition any => :ready, :do => :_finish_previous_query

      # when the bind is done, describe and execute the portal
      after_transition :bind_in_progress => :bind_completed, :do => :_send_portal_describe
      after_transition :portal_describe_in_progress => :portal_described, :do => :_send_execute

      event :authentication_ok do
        transition [:startup, :authorizing] => :receive_server_data
      end
      event :authentication_kerberos_v5 do
        transition :startup => :unsupported_auth_method
      end
      event :authentication_cleartext_password do
        transition :startup => :authorizing
      end
      event :authentication_md5_password do
        transition :startup => :authorizing
      end
      event :authentication_scm_credential do
        transition :startup => :unsupported_auth_method
      end
      event :authentication_gss do
        transition :startup => :unsupported_auth_method
      end
      event :authentication_gss_continue do
        transition :startup => :unsupported_auth_method
      end
      event :authentication_sspi do
        transition :startup => :unsupported_auth_method
      end

      event :backend_key_data do
        transition :receive_server_data => same
      end
      event :parameter_status do
        transition :receive_server_data => same
      end
      event :notice_response do
      end
      event :error_response do
        transition [:startup, :authorizing] => :connection_failed
        transition [:query_in_progress, :query_described, :query_data_received] => :query_failed
        transition :parse_in_progress => :parse_failed
        transition :bind_in_progress => :bind_failed
        transition :execute_in_progress => :execute_failed
      end
      event :error_reset do
        transition :parse_failed => :ready
        transition :bind_failed => :ready
      end
      event :ready_for_query do
        transition [:receive_server_data, :query_in_progress] => :ready
        transition :query_failed => :ready
      end

      event :query_sent do
        transition :ready => :query_in_progress
      end
      event :parse_sent do
        transition :ready => :parse_in_progress
      end
      event :bind_sent do
        transition :ready => :bind_in_progress
      end
      event :portal_describe_sent do
        transition :bind_completed => :portal_describe_in_progress
      end
      event :execute_sent do
        transition :portal_described => :execute_in_progress
      end
      event :sync_sent do
      end

      event :command_complete do
        transition [:query_in_progress, :query_described, :query_data_received] => :query_in_progress
        transition :execute_in_progress => :ready
      end
      event :parse_complete do
        transition :parse_in_progress => :ready
      end
      event :bind_complete do
        transition :bind_in_progress => :bind_completed
      end
      event :row_description do
        transition :query_in_progress => :query_described
        transition :portal_describe_in_progress => :portal_described
      end
      event :data_row do
        transition [:query_described, :query_data_received] => :query_data_received
        transition :execute_in_progress => same
      end
      event :portal_suspended do
        transition :execute_in_progress => same
      end
      event :empty_query_response do
        transition :query_in_progress => same
        transition :execute_in_progress => :ready
      end

      #
      # In the ready state, the query methods send the requests to PostgreSQL
      # immediately.
      #
      state :ready do
        def execute(sql)
          @logger.debug 'in #execute; starting immediate'
          _send(:_send_query, [sql])
        end

        def prepare(name, sql, datatypes = nil)
          @logger.debug 'in #prepare; starting immediate'
          _send(:_send_parse, [name, sql, datatypes])
        end

        def execute_prepared(name, *bindvars)
          @logger.debug 'in #execute_prepared; starting immediate'
          _send(:_send_bind, [name, *bindvars])
        end
      end

      #
      # In all other states, the query methods queue the requests until the
      # next time the ready state is entered.
      #
      state all - :ready do
        def execute(sql)
          @logger.debug 'in #execute; queueing request'
          defer = ::EM::DefaultDeferrable.new
          @waiting << [:_send_query, [sql], defer]
          defer
        end

        def prepare(name, sql, datatypes = nil)
          @logger.debug 'in #prepare; queueing request'
          defer = ::EM::DefaultDeferrable.new
          @waiting << [:_send_parse, [name, sql, datatypes], defer]
          defer
        end

        def execute_prepared(name, *bindvars)
          @logger.debug 'in #execute_prepared; queueing request'
          defer = ::EM::DefaultDeferrable.new
          @waiting << [:_send_bind, [name, *bindvars], defer]
          defer
        end
      end

    end

    # Returns the PostgreSQL server parameters
    #
    # @return [Hash]
    attr_reader :server_params

    # Sets or retrieves the batch size used by +#execute_prepared+
    #
    # A value of zero means there is no limit.
    #
    # @return [Integer]
    attr_reader :batch_size

    # Returns the logger passed to +Connection::connect+
    attr_reader :logger

    #
    # Initiates a connection to PostgreSQL and returns a +Deferrable+
    #
    # If the connection and authentication are successful, the +Connection+
    # object will be sent to the +Deferrable+'s +#callback+ method. If it
    # fails, the text of the error is sent to the +#errback+ method.
    #
    # @option opts [String] :host The hostname or IP address of the PostgreSQL
    #   server (if not specified, connects via the UNIX domain socket)
    # @option opts [Integer] :port The port (defaults to 5432)
    # @option opts [String] :dbname The database name
    # @option opts [String] :user The user name
    # @option opts [String] :password The password
    # @option opts [#trace, #debug, #info, #warn, #error, #fatal] :logger A logging object
    # @return [EventMachine::Deferrable]
    #
    def self.connect(opts = {})
      host = opts[:host]
      port = opts[:port] || Default_Port
      defer = ::EM::DefaultDeferrable.new
      if host
        ::EM.connect(host, port, self, defer, opts)
      else
        ::EM.connect_unix_domain("/tmp/.s.PGSQL.#{port}", self, defer, opts)
      end
      defer
    end

    def initialize(defer, opts = {}) # @private
      super()
      @dbname = opts[:dbname]
      @user = opts[:user] || Etc.getlogin
      @password = opts[:password]
      @buffer = MosesPG::Message::Buffer.new
      @in_progress = defer
      @logger = opts[:logger] || NullLogger.new
      @batch_size = 0
      @server_params = {}
      @portals = {}
      @waiting = []
    end

    def batch_size=(batch_size)
      @batch_size = batch_size.to_i
    end

    #
    # Called by +EventMachine::Connection+ once the connection is established
    #
    # @return [Connection]
    #
    def post_init
      send_message(MosesPG::Message::StartupMessage.new(@user, @dbname))

      # when we enter the ready state after connecting, send self to the
      # callback
      @result = FakeResult.new(self)
      self
    end

    #
    # Sends a +Message+ to the PostgreSQL server
    #
    # @param [Message] message The +Message+ object to send to the server
    #
    # @return [Connection]
    #
    def send_message(message)
      @logger.trace { "<<< #{message}" }
      s = message.dump
      @logger.trace { "<<< #{s.inspect} [#{s.size}] (#{s.encoding})" }
      send_data(message.dump)
      self
    end

    #
    # Called by +EventMachine::Connection+ when data is received
    #
    # @return [Connection]
    #
    def receive_data(data)
      #@logger.trace { ">>> #{data.inspect}" }
      @buffer.receive(data) do |message_type_char, raw_message|
        @message = MosesPG::Message.create(message_type_char, raw_message)
        @logger.trace { ">>> #{@message}" }
        send(@message.event, @message)
      end
      self
    end

    #
    # Called by +EventMachine::Connection+ when the connection is closed
    #
    # @return [Connection]
    #
    def unbind
      @logger.debug 'Connection closed'
      self
    end

    #
    # Submits SQL command (or commands) to the PostgreSQL server and returns a
    # +Deferrable+
    #
    # If all commands are successfully executed, then the +Result+ is sent to
    # the +Deferrable+'s +#callback+ method.  If an error occurs, an error
    # message is sent to +#errback+, along with any partial +Result+ that
    # accumulated before the error.
    #
    # @param [String] sql A single SQL command or multiple commands, separated
    #   by semicolons
    # @return [EventMachine::Deferrable]
    #
    def execute(sql)
      super
    end

    #
    # Submits a single SQL command for parsing and returns a +Deferrable+
    #
    # @param [String] name A name used to refer to the SQL later for execution
    # @param [String] sql A single SQL command to be parsed and saved
    # @param [nil, Array<Integer>] datatypes The data type(s) to use for the parameters
    #   included in the SQL command, if any
    # @return [EventMachine::Deferrable]
    #
    def prepare(name, sql, datatypes = nil)
      super
    end

    #
    # Initiates the execution of a previously prepared SQL command and returns
    # a +Deferrable+
    #
    # If the command is successfully executed, then the +Result+ is sent to the
    # +Deferrable+'s +#callback+ method.  If an error occurs, an error message
    # is sent to +#errback+, along with any partial +Result+ that accumulated
    # before the error.
    #
    # @param [String] name The name given to the SQL command when +#prepare+
    #   was called
    # @param [Array<Object>] bindvars The values to bind to the placeholders in
    #   the SQL command, if any
    # @return [EventMachine::Deferrable]
    #
    def execute_prepared(name, *bindvars)
      super
    end

    private

    def authentication_cleartext_password(*args)
      send_message(MosesPG::Message::PasswordMessage.new(@password))
      super
    end

    def authentication_md5_password(*args)
      hash1 = Digest::MD5.hexdigest(@password + @user)
      hash2 = Digest::MD5.hexdigest(hash1 + @message.salt)
      send_message(MosesPG::Message::PasswordMessage.new('md5' + hash2))
      super
    end

    def parameter_status(*args)
      @logger.debug { "in #parameter_status" }
      @server_params[@message.name] = @message.value
      super
    end

    def backend_key_data(*args)
      @server_process_id = @message.process_id
      @secret_key = @message.secret_key
      super
    end

    def command_complete(*args)
      @result.finish(@message.tag)
      super
    end

    def row_description(*args)
      @result.columns = @message.columns
      super
    end

    def data_row(*args)
      @result << @message.row
      super
    end

    def _finish_previous_query
      @logger.debug { "entering #_finish_previous_query; @in_progress = #{@in_progress.__id__}" }
      # Create a closure w/the current values, since calling #succeed MUST
      # follow checking the queue
      last_succeeded = if @in_progress
        last_defer = @in_progress
        last_result = @result
        proc do
          # For EM::Synchrony#sync to work its magic, code that calls the bang
          # methods must occur in the context of a Fiber. Code executed in
          # EventMachine callbacks (such as in #succeed, #fail, #next_tick,
          # #add_timer) is outside the context of the Fiber created in
          # EM::synchrony, and so must be given another Fiber context.
          Fiber.new { last_defer.succeed(last_result.nil? ? nil : last_result.result) }.resume
        end
      end

      # Reset and process the next command in the queue. If there is one, this
      # will change the state from ready, which is why we have to call #succeed
      # on the previous query afterward.
      @in_progress = @result = nil
      unless @waiting.empty?
        action, args, defer = @waiting.shift
        _send(action, args, defer)
      end

      # call succeed on the previous deferrable
      if last_succeeded
        @logger.debug { "calling #succeed for previous Deferrable" }
        #EM.next_tick {
        last_succeeded.call #}
      end
      @logger.debug 'leaving #_finish_previous_query'
    end

    def _send(action, args, defer = nil)
      @logger.debug { "entering #_send(#{action.inspect}, ...)" }
      @in_progress = defer || ::EM::DefaultDeferrable.new
      send(action, *args)
      @logger.debug { "leaving #_send(#{action.inspect}, ...); @in_progress = #{@in_progress.__id__}" }
      @in_progress
    end

    def _send_query(sql)
      send_message(MosesPG::Message::Query.new(sql))
      @result = MosesPG::ResultGroup.new(self)
      query_sent
    end

    def _send_parse(name, sql, datatypes = nil)
      send_message(MosesPG::Message::Parse.new(name, sql, datatypes))
      send_message(MosesPG::Message::Flush.new)
      parse_sent
    end

    def _send_bind(statement_name, *bindvars)
      @statement_in_progress = statement_name
      send_message(MosesPG::Message::Bind.new(statement_name, generate_portal_name(statement_name), bindvars, nil, nil))
      send_message(MosesPG::Message::Flush.new)
      bind_sent
    end

    def _send_portal_describe
      send_message(MosesPG::Message::DescribePortal.new(@portals[@statement_in_progress.to_s]))
      send_message(MosesPG::Message::Flush.new)
      @result = MosesPG::Result.new(self)
      portal_describe_sent
    end

    def _send_execute
      send_message(MosesPG::Message::Execute.new(@portals[@statement_in_progress.to_s], @batch_size))
      send_message(MosesPG::Message::Flush.new)
      execute_sent
    end

    def _fail_connection
      @in_progress.fail(@message.errors['M'])
    end
    alias :_fail_parse :_fail_connection
    alias :_fail_bind :_fail_connection

    def _fail_query
      @in_progress.fail(@message.errors['M'], @result.result)
    end
    alias :_fail_execute :_fail_query

    def generate_portal_name(statement_name)
      str = statement_name.to_s
      @portals[str] = "__P__#{str}"
    end

  end

end
