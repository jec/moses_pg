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

module MosesPG

  class Error < RuntimeError
    def initialize(message, severity, detail = nil, hint = nil, position = nil)
      super(message)
      @severity = severity
      @detail = detail
      @hint = hint
      @position = position ? position.to_i : nil
    end
    def inspect
      buf = [message]
      buf.unshift(@severity) if @severity
      buf << "(#{@detail})" if @detail
      buf << "[#{@hint}]" if @hint
      "#<#{self.class.name} #{buf.join(' ')}>"
    end
  end

end
