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

require 'state_machine'

module MosesPG
  module StateMachine
    def self.included(klass)
      klass.instance_eval do

        state_machine :initial => :startup do
          # log all transitions
          after_transition any => any do |obj, trans|
            obj.logger.trace { "+++ On event #{trans.event}: #{trans.from_name} => #{trans.to_name}" }
          end

          # entering a failure state fails the query
          after_transition any => :connection_failed, :do => :fail_connection
          after_transition any => :query_failed, :do => :fail_query
          after_transition any => :parse_failed, :do => :fail_parse
          after_transition any => :bind_failed, :do => :fail_bind
          after_transition any => :execute_failed, :do => :fail_execute
          after_transition any => :close_portal_failed, :do => :fail_close_portal
          after_transition any => :close_statement_failed, :do => :fail_close_statement

          # entering the ready state checks the query queue and calls succeed for
          # the previous command
          after_transition any => :ready, :do => :finish_previous_query

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
            transition any => same
          end
          event :error_response do
            transition [:startup, :authorizing] => :connection_failed
            transition [:query_in_progress, :rowset_query_in_progress, :empty_query_in_progress] => :query_failed
            transition :parse_in_progress => :parse_failed
            transition :bind_in_progress => :bind_failed
            transition :execute_in_progress => :execute_failed
            transition :close_portal_in_progress => :close_portal_failed
            transition :close_statement_in_progress => :close_statement_failed
          end
          event :error_reset do
            transition [:parse_failed, :bind_failed] => :syncing
          end
          event :ready_for_query do
            transition [:receive_server_data, :query_in_progress, :empty_query_in_progress, :query_failed, :syncing] => :ready
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
          event :describe_statement_sent do
            transition :ready => :statement_describe_in_progress
          end
          event :describe_portal_sent do
            transition :ready => :portal_describe_in_progress
          end
          event :execute_sent do
            transition :ready => :execute_in_progress
          end
          event :close_portal_sent do
            transition :ready => :close_portal_in_progress
          end
          event :close_statement_sent do
            transition :ready => :close_statement_in_progress
          end
          event :sync_sent do
          end

          event :command_complete do
            transition [:query_in_progress, :rowset_query_in_progress] => :query_in_progress
            transition :execute_in_progress => :ready
          end
          event :parse_complete do
            transition :parse_in_progress => :ready
          end
          event :bind_complete do
            transition :bind_in_progress => :ready
          end
          event :close_complete do
            transition [:close_portal_in_progress, :close_statement_in_progress] => :ready
          end
          event :parameter_description do
            transition :statement_describe_in_progress => same
          end
          event :row_description do
            transition [:query_in_progress, :rowset_query_in_progress] => :rowset_query_in_progress
            transition [:statement_describe_in_progress, :portal_describe_in_progress] => :ready
          end
          event :data_row do
            transition [:query_in_progress, :rowset_query_in_progress] => :rowset_query_in_progress
            transition :execute_in_progress => same
          end
          event :no_data do
            transition [:statement_describe_in_progress, :portal_describe_in_progress] => :ready
          end
          event :portal_suspended do
            transition :execute_in_progress => same
          end
          event :empty_query_response do
            transition :query_in_progress => :empty_query_in_progress
            transition :execute_in_progress => :ready
          end

          #
          # In the ready state, the query methods send the requests to PostgreSQL
          # immediately.
          #
          state :ready do
            def execute(sql)
              @logger.trace 'in #execute; starting immediate'
              _send(:_send_query, [sql])
            end

            def commit
              @logger.trace 'in #commit; starting immediate'
              _send(:_send_query_message, [@commit_msg])
            end

            def rollback
              @logger.trace 'in #rollback; starting immediate'
              _send(:_send_query_message, [@rollback_msg])
            end

            def _start_transaction
              @logger.trace 'in #_start_transaction; starting immediate'
              _send(:_send_query_message, [@start_xact_msg])
            end

            def _prepare(name, sql, datatypes = nil)
              @logger.trace 'in #_prepare; starting immediate'
              _send(:_send_parse, [name, sql, datatypes])
            end

            def _bind(statement, bindvars)
              @logger.trace 'in #_bind; starting immediate'
              _send(:_send_bind, [statement, bindvars])
            end

            def _describe_statement(statement)
              @logger.trace 'in #_describe_statement; starting immediate'
              _send(:_send_describe_statement, [statement])
            end

            def _describe_portal(statement)
              @logger.trace 'in #_describe_portal; starting immediate'
              _send(:_send_describe_portal, [statement])
            end

            def _execute(statement)
              @logger.trace 'in #_execute; starting immediate'
              _send(:_send_execute, [statement])
            end

            def _close_portal(statement)
              @logger.trace 'in #_close_portal; starting immediate'
              _send(:_send_close_portal, [statement])
            end

            def _close_statement(statement)
              @logger.trace 'in #_close_statement; starting immediate'
              _send(:_send_close_statement, [statement])
            end
          end

          #
          # In all other states, the query methods queue the requests until the
          # next time the ready state is entered.
          #
          state all - :ready do
            def execute(sql)
              @logger.trace 'in #execute; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_query, [sql], defer]
              defer
            end

            def commit
              @logger.trace 'in #commit; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_query_message, [@commit_msg], defer]
              defer
            end

            def rollback
              @logger.trace 'in #rollback; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_query_message, [@rollback_msg], defer]
              defer
            end

            def _start_transaction
              @logger.trace 'in #_start_transaction; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_query_message, [@start_xact_msg], defer]
              defer
            end

            def _prepare(name, sql, datatypes = nil)
              @logger.trace 'in #_prepare; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_parse, [name, sql, datatypes], defer]
              defer
            end

            def _bind(statement, bindvars)
              @logger.trace 'in #_bind; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_bind, [statement, bindvars], defer]
              defer
            end

            def _describe_statement(statement)
              @logger.trace 'in #_describe_statement; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_describe_statement, [statement], defer]
              defer
            end

            def _describe_portal(statement)
              @logger.trace 'in #_describe_portal; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_describe_portal, [statement], defer]
              defer
            end

            def _execute(statement)
              @logger.trace 'in #_execute; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_execute, [statement], defer]
              defer
            end

            def _close_portal(statement)
              @logger.trace 'in #_close_portal; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_close_portal, [statement], defer]
              defer
            end

            def _close_statement(statement)
              @logger.trace 'in #_close_statement; queueing request'
              defer = ::EM::DefaultDeferrable.new
              @waiting << [:_send_close_statement, [statement], defer]
              defer
            end
          end
        end

      end
    end
  end
end
