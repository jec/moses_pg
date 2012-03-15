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

module MosesPG
  module Message

    class Buffer

      attr_reader :message_type_char, :expected_size, :current_size

      def initialize
        reset
      end

      def reset(data = nil)
        @message_type_char = nil
        @expected_size = nil
        if data
          if data.size >= 5
            @message_type_char = data[0]
            @expected_size = data[1..4].unpack('N').first - 4
            data = data[5..-1]
          end
          @current_size = data.size
          @buffer = [data]
        else
          @current_size = 0
          @buffer = []
        end
      end

      def receive(data)
        @buffer << data
        @current_size += data.size
        if @expected_size.nil? && @current_size >= 5
          reset(@buffer.join)
        end
        while !@expected_size.nil? && @current_size >= @expected_size
          ch = @message_type_char
          str = @buffer.join
          if @current_size == @expected_size
            reset
            yield(ch, str)
          else
            str1 = str[0, @expected_size]
            reset(str[@expected_size..-1])
            yield(ch, str1)
          end
        end
      end

      def flush
        result = (@message_type_char.nil? ? '' : @message_type_char) +
            (@expected_size.nil? ? '' : [@expected_size + 4].pack('N')) + @buffer.join
        reset
        result
      end

    end

  end
end
