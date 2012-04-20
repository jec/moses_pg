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
  module TransactionStateMachine
    def self.included(klass)
      klass.instance_eval do

        state_machine :transaction_state, :namespace => 'tx', :initial => :none do
          after_transition any => any do |obj, trans|
            obj.logger.trace { "+++ On TX event #{trans.event}: #{trans.from_name} => #{trans.to_name}" }
          end

          #after_transition :start_pending => :active, :do => :init_transaction
          after_transition any => :none, :do => :close_transaction

          event :start do
            transition :none => :start_pending
          end
          event :commit do
            transition :active => :commit_pending
          end
          event :rollback do
            transition :active => :rollback_pending
          end
          event :command_complete do
            transition :start_pending => :active
            transition :commit_pending => :none
            transition :rollback_pending => :none
          end

          state :none do
            def _run(name, args, tx)
              @logger.trace { "in #_run: running #{name.inspect} immediate" }
              _send(name, args)
            end

            def _enqueue(name, args, tx)
              @logger.trace { "in #_enqueue: putting #{name.inspect} into this queue" }
              deferrable = ::EM::DefaultDeferrable.new
              @this_tx_q << [name, args, deferrable]
              deferrable
            end
          end

          state any - :none do
            def _run(name, args, tx)
              if tx == @transaction
                @logger.trace { "in #_run: running #{name.inspect} immediate" }
                _send(name, args)
              else
                @logger.trace { "in #_run: putting #{name.inspect} into next queue" }
                deferrable = ::EM::DefaultDeferrable.new
                @next_tx_q << [name, args, deferrable]
                deferrable
              end
            end

            def _enqueue(name, args, tx)
              deferrable = ::EM::DefaultDeferrable.new
              queue = if tx == @transaction
                @logger.trace { "in #_enqueue: putting #{name.inspect} into this queue" }
                @this_tx_q
              else
                @logger.trace { "in #_enqueue: putting #{name.inspect} into next queue" }
                @next_tx_q
              end
              queue << [name, args, deferrable]
              deferrable
            end
          end
        end
      end
    end
  end
end
