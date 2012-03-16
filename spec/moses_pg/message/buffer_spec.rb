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

require 'moses_pg/message/buffer'

class String
  def to_message(c)
    c[0] + [size + 4].pack('N') + self
  end
end

module MosesPG::Message

  describe Buffer do

    before(:each) do
      @buffer = Buffer.new
    end

    def pieces(c, str, count)
      result = []
      msg = str.to_message(c)
      width = msg.size / count
      count.times do |i|
        if i == count - 1
          result << msg[i*width .. -1]
        else
          result << msg[i*width .. (i+1)*width-1]
        end
      end
      #puts "#{str.inspect} => #{result.inspect}"
      result
    end

    describe '#receive' do
      describe 'when receiving partial messages' do
        describe 'when partial messages fall on message boundaries' do
          it 'waits and yields the entire messages' do
            p1, p2, p3 = pieces('A', 'this is a test', 3)
            @buffer.receive(p1) { |c, s| fail }
            @buffer.receive(p2) { |c, s| fail }
            @buffer.receive(p3) do |c, s|
              c.should == 'A'
              s.should == 'this is a test'
            end

            p4, p5, p6 = pieces('B', 'this is yet another test', 3)
            @buffer.receive(p4) { |c, s| fail }
            @buffer.receive(p5) { |c, s| fail }
            @buffer.receive(p6) do |c, s|
              c.should == 'B'
              s.should == 'this is yet another test'
            end
          end
        end

        describe 'when partial messages do not fall on message boundaries' do
          it 'yields only the entire messages' do
            p1, p2, p3 = pieces('C', 'this is a test', 3)
            p4, p5, p6 = pieces('D', 'this is yet another test', 3)
            p7, p8, p9 = pieces('E', 'now is the time', 3)
            @buffer.receive(p1) { |c, s| fail }
            @buffer.receive(p2) { |c, s| fail }
            @buffer.receive(p3 + p4) do |c, s|
              c.should == 'C'
              s.should == 'this is a test'
            end
            @buffer.receive(p5) { |c, s| fail }
            @buffer.receive(p6 + p7) do |c, s|
              c.should == 'D'
              s.should == 'this is yet another test'
            end
            @buffer.receive(p8) { |c, s| fail }
            @buffer.receive(p9) do |c, s|
              c.should == 'E'
              s.should == 'now is the time'
            end
          end
        end

        describe 'when partial messages are shorter than the 5-byte prefix' do
          it 'yields only the entire messages' do
            pieces = 'this is a test'.to_message('F').scan(/../)
            pieces[0..-2].each do |piece|
              @buffer.receive(piece) { |c, s| fail }
            end
            @buffer.receive(pieces.last) do |c, s|
              c.should == 'F'
              s.should == 'this is a test'
            end
          end
        end
      end

      describe 'when receiving multiple messages' do
        it 'yields only the entire messages' do
          m1 = 'this is a test'.to_message('G')
          m2 = 'this is yet another test'.to_message('H')
          buffer = []
          @buffer.receive(m1 + m2) { |c, s| buffer << [c, s] }
          buffer.should == [['G', 'this is a test'], ['H', 'this is yet another test']]
        end
      end
    end

    describe '#flush' do
      describe 'when buffer is empty' do
        it 'returns an empty string' do
          @buffer.flush.should == ''
          @buffer.expected_size.should be_nil
          @buffer.current_size.should == 0
        end
      end

      describe 'when buffer has less than the 5-byte prefix' do
        it 'returns the raw bytes' do
          piece = "X\0\0"
          @buffer.receive(piece)
          @buffer.flush.should == piece
          @buffer.message_type_char.should be_nil
          @buffer.expected_size.should be_nil
          @buffer.current_size.should == 0
        end
      end

      describe 'when buffer has a partial message' do
        it 'returns the partial' do
          p1, p2 = pieces('I', 'this is a test', 2)
          @buffer.receive(p1)
          @buffer.flush.should == p1
          @buffer.message_type_char.should be_nil
          @buffer.expected_size.should be_nil
          @buffer.current_size.should == 0
        end
      end
    end

  end

end
