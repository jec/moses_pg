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

require 'etc'
require 'eventmachine'
require 'moses_pg/state_machine'
require 'moses_pg/transaction_state_machine'
require 'moses_pg/message'
require 'moses_pg/message/buffer'
require 'moses_pg/result'
require 'moses_pg/statement'
require 'moses_pg/transaction'

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
  # If you intend to call any of the bang methods, then you must require
  # +moses_pg/sync+ and start EventMachine's reactor using +EM::synchrony+
  # instead of +EM::run+. If you use +EM::run+, you will get the error
  # <em>"can't yield from root fiber"</em> when you call a bang method.
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

    include StateMachine
    include TransactionStateMachine

    Default_Host = 'localhost'
    Default_Port = 5432

    # Returns the PostgreSQL server parameters
    #
    # @return [Hash]
    attr_reader :server_params

    # Sets or retrieves the batch size used by +#execute+
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
    # @option opts [#trace] :logger A logging object with a #trace method
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
      @logger = opts[:logger] || NullLogger.new
      @batch_size = 0
      @server_params = {}
      @this_tx_q = []
      @in_progress = defer

      @start_xact_msg = Message::Query.new('START TRANSACTION')
      @commit_msg = Message::Query.new('COMMIT')
      @rollback_msg = Message::Query.new('ROLLBACK')
    end

    def batch_size=(batch_size)
      @batch_size = batch_size.to_i
    end

    #
    # Called by +EventMachine::Connection+ once the connection is established
    #
    # @return [MosesPG::Connection]
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
    # @return [MosesPG::Connection]
    #
    def send_message(message)
      @logger.trace { "<<< #{message}" }
      s = message.dump
      #@logger.trace { "<<< #{s.inspect} [#{s.size}] (#{s.encoding})" }
      send_data(message.dump)
      self
    end

    #
    # Called by +EventMachine::Connection+ when data is received
    #
    # @return [MosesPG::Connection]
    #
    def receive_data(data)
      #@logger.trace { ">>> #{data.inspect}" }
      @buffer.receive(data) do |message_type_char, raw_message|
        @message = MosesPG::Message.create(message_type_char, raw_message)
        @logger.trace { ">>> #{@message}" }
        @message.events.each { |event| send(event) }
      end
      self
    end

    #
    # Called by +EventMachine::Connection+ when the connection is closed
    #
    # @return [MosesPG::Connection]
    #
    def unbind
      @logger.trace 'Connection closed'
      self
    end

    def transaction
      if block_given?
        deferrable = ::EM::DefaultDeferrable.new
        tx = Transaction.new(self)
        defer0 = _start_transaction(tx)
        defer0.callback do
          defer1 = nil
          begin
            do_rollback = proc do |e|
              d = rollback(tx)
              d.callback { deferrable.fail(e) }
              d.errback { deferrable.fail(e) }
            end
            case defer1 = yield(tx)
            when ::EM::Deferrable
              defer1.callback do |*args|
                defer2 = commit(tx)
                defer2.callback { deferrable.succeed(*args) }
                defer2.errback { |err| deferrable.fail(err) }
              end
              defer1.errback { |err| do_rollback.call(err) }
            else
              e = RuntimeError.new("Block must return a Deferrable, but it returned #{defer1.class.name}")
              e.set_backtrace(caller)
              do_rollback.call(e)
            end
          rescue Exception => e
            do_rollback.call(e)
          end
        end
        defer0.errback { |err| deferrable.fail(err) }
        deferrable
      else
        raise "No block given for #transaction"
      end
    end

    def commit(tx = nil)
      run(:_send_commit, [], tx)
    end

    def rollback(tx = nil)
      run(:_send_rollback, [], tx)
    end

    def _start_transaction(tx = nil)
      run(:_send_start_transaction, [tx], tx)
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
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def execute(sql, tx = nil)
      run(:_send_query, [sql], tx)
    end

    #
    # Submits a single SQL command for parsing and returns a +Deferrable+
    #
    # @param [String] sql A single SQL command to be parsed and saved
    # @param [nil, Array<Integer>] datatypes The data type(s) to use for the parameters
    #   included in the SQL command, if any
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def prepare(sql, datatypes = nil, tx = nil)
      Statement.prepare(self, sql, datatypes, tx)
    end

    #
    # Submits a single SQL command for parsing and returns a +Deferrable+
    #
    # This method is intended for use by +Statement::create+.
    #
    # @api private
    # @param [String] name A name used to refer to the SQL later for execution
    # @param [String] sql A single SQL command to be parsed and saved
    # @param [nil, Array<Integer>] datatypes The data type(s) to use for the parameters
    #   included in the SQL command, if any
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _prepare(name, sql, datatypes = nil, tx = nil)
      run(:_send_parse, [name, sql, datatypes], tx)
    end

    #
    # This method is intended for use by +Statement::bind+.
    #
    # @api private
    # @param [MosesPG::Statement] statement The +Statement+ object being bound to
    # @param [Array<Object>] bindvars The values being bound
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _bind(statement, bindvars, tx = nil)
      @statement = statement
      run(:_send_bind, [statement, bindvars], tx)
    end

    #
    # This method is intended for use by +Statement::bind+.
    #
    # @api private
    # @param [MosesPG::Statement] statement The +Statement+ object
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _describe_statement(statement, tx = nil)
      @statement = statement
      run(:_send_describe_statement, [statement], tx)
    end

    #
    # This method is intended for use by +Statement::bind+.
    #
    # @api private
    # @param [MosesPG::Statement] statement The +Statement+ object
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _describe_portal(statement, tx = nil)
      @statement = statement
      run(:_send_describe_portal, [statement], tx)
    end

    #
    # This method is intended for use by +Statement::execute+.
    #
    # @api private
    # @param [MosesPG::Statement] statement The +Statement+ object being executed
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _execute(statement, tx = nil)
      @statement = statement
      super
    end

    #
    # Initiates the closing of a +Statement+'s portal and returns a
    # +Deferrable+
    #
    # This method is intended for use by +Statement::close+.
    #
    # @api private
    # @param [MosesPG::Statement] statement The +Statement+ object
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _close_portal(statement, tx = nil)
      @statement = statement
      run(:_send_close_portal, [statement], tx)
    end

    #
    # Initiates the closing of a +Statement+ and returns a +Deferrable+
    #
    # This method is intended for use by +Statement::close+.
    #
    # @api private
    # @param [MosesPG::Statement] statement The +Statement+ object being closed
    # @param [MosesPG::Transaction] tx The +Transaction+, if any
    # @return [EventMachine::Deferrable]
    #
    def _close_statement(statement, tx = nil)
      @statement = statement
      run(:_send_close_statement, [statement], tx)
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

    def parameter_description(*args)
      @result.set_raw_parameters(@message.oids)
      super
    end

    def row_description(*args)
      @result.set_raw_columns(@message.columns)
      super
    end

    def data_row(*args)
      @result << @message.row
      super
    end

    def notice_response(*args)
      @result.add_notice(@message.fields)
      super
    end

    def finish_previous_query
      @logger.trace('entering #finish_previous_query')
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
      process_queue

      # call succeed on the previous deferrable
      if last_succeeded
        @logger.trace('calling #succeed for previous Deferrable')
        EM.next_tick { last_succeeded.call }
      end
      @logger.trace('leaving #finish_previous_query')
    end

    def init_transaction
    end

    def close_transaction
      pop_queue
      @transaction = nil
    end

    def push_queue
      @logger.trace('... pushing queue')
      @next_tx_q = @this_tx_q
      @this_tx_q = []
    end

    def pop_queue
      @logger.trace('... popping queue')
      raise "Popping non-empty queue" unless @this_tx_q.empty?
      @this_tx_q = @next_tx_q
      @next_tx_q = nil
    end

    def _send(action, args, defer = nil)
      #@logger.trace { "entering #_send(#{action.inspect}, ...)" }
      @in_progress = defer || ::EM::DefaultDeferrable.new
      send(action, *args)
      #@logger.trace { "leaving #_send(#{action.inspect}, ...)" }
      @in_progress
    end

    def _send_query(sql)
      send_message(MosesPG::Message::Query.new(sql))
      @result = MosesPG::ResultGroup.new(self)
      query_sent
    end

    def _send_start_transaction(tx)
      send_message(@start_xact_msg)
      @result = MosesPG::ResultGroup.new(self)
      @transaction = tx
      push_queue
      start_tx
      query_sent
    end

    def _send_commit
      send_message(@commit_msg)
      @result = MosesPG::ResultGroup.new(self)
      commit_tx
      query_sent
    end

    def _send_rollback
      send_message(@rollback_msg)
      @result = MosesPG::ResultGroup.new(self)
      rollback_tx
      query_sent
    end

    def _send_parse(name, sql, datatypes = nil)
      send_message(MosesPG::Message::Parse.new(name, sql, datatypes))
      send_message(MosesPG::Message::Flush.instance)
      parse_sent
    end

    def _send_bind(statement, bindvars)
      send_message(MosesPG::Message::Bind.new(statement.name, statement.portal_name, bindvars, statement.result_format_codes))
      send_message(MosesPG::Message::Flush.instance)
      bind_sent
    end

    def _send_describe_statement(statement)
      send_message(MosesPG::Message::DescribeStatement.new(statement.name))
      send_message(MosesPG::Message::Flush.instance)
      @result = MosesPG::Result.new(self)
      describe_statement_sent
    end

    def _send_describe_portal(statement)
      send_message(MosesPG::Message::DescribePortal.new(statement.portal_name))
      send_message(MosesPG::Message::Flush.instance)
      @result = MosesPG::Result.new(self)
      describe_portal_sent
    end

    def _send_execute(statement)
      send_message(MosesPG::Message::Execute.new(statement.portal_name, @batch_size))
      send_message(MosesPG::Message::Flush.instance)
      @result = MosesPG::Result.new(self)
      execute_sent
    end

    def _send_close_portal(statement)
      send_message(MosesPG::Message::ClosePortal.new(statement.portal_name))
      send_message(MosesPG::Message::Flush.instance)
      close_portal_sent
    end

    def _send_close_statement(statement)
      send_message(MosesPG::Message::CloseStatement.new(statement.name))
      send_message(MosesPG::Message::Flush.instance)
      close_statement_sent
    end

    def fail_connection
      @in_progress.fail(@message.make_exception)
    end

    def fail_parse
      @in_progress.fail(@message.make_exception)
      send_message(MosesPG::Message::Sync.instance)
      error_reset
    end
    alias :fail_bind :fail_parse
    alias :fail_close_portal :fail_parse
    alias :fail_close_statement :fail_parse

    def fail_query
      @in_progress.fail(@message.make_exception, @result.result)
      @in_progress = nil
    end

    def fail_execute
      @in_progress.fail(@message.make_exception, @result.result)
      @in_progress = nil
      send_message(MosesPG::Message::Sync.instance)
    end

  end

end
