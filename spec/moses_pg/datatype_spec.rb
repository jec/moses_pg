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

require 'moses_pg/datatype'

Value = Struct.new(:oid, :mod, :sql_type, :mpg_class_name, :literal, :expected)

module MosesPG

  module Datatype

    Types = [
      # OID      Mod  SQL type            MPG class name     text literal              expected native value
      # numeric
      [  21,      -1, 'smallint',         :Smallint,                  '12',                      12           ],
      [  23,      -1, 'integer',          :Integer,                  '123',                     123           ],
      [  20,      -1, 'bigint',           :Bigint,          '123456789012',            123456789012           ],
      [1700, 1310734, 'numeric(20,10)',   :Numeric_20_10,     '1234567890.1234567890',   1234567890.1234567890],
      [ 700,      -1, 'real',             :Float4,                '123456.789012',           123456.789012    ],
      [ 701,      -1, 'double precision', :Float8,                '123456.7890123456',       123456.7890123456],
      # monetary
      [ 790,      -1, 'money',            :Money,                    '$12.34',                 '$12.34'       ],
      # character
      [1043,      34, 'varchar(30)',      :Varchar_30,      'Hello world',             'Hello world'          ],
      [1042,      14, 'char(10)',         :BPChar_10,       'Hello     ',              'Hello     '           ],
      [  25,      -1, 'text',             :Text,            'Hello world',             'Hello world'          ],
      # binary
      [  17,      -1, 'bytea',            :Bytea,         "\\x48656c6c6f20776f726c64", "\\x48656c6c6f20776f726c64"],
      # date/time
      [1082,      -1, 'date',             :Date,          '2012-03-14',                ::Date.new(2012, 3, 14)],
      [1083,      -1, 'time',             :Time,          '12:34:56.789012',           '12:34:56.789012'      ],
      [1266,      -1, 'time with time zone', :TimeTZ,     '12:34:56.789012-04',        '12:34:56.789012-04'   ],
      [1114,      -1, 'timestamp',        :Timestamp,     '2012-03-14 12:34:56.789012',::Time.local(2012,3,14,12,34,56,789012)],
      [1184,      -1, 'timestamp with time zone',:TimestampTZ,'2012-03-14 12:34:56.789012-04',
                                                                               ::Time.parse('2012-03-14 12:34:56.789012-04',)],
      [1186,      -1, 'interval',         :Interval,      '01:23:45.678901',           '01:23:45.678901'      ],
      # boolean
      [  16,      -1, 'boolean',          :Boolean,       't',                         true                   ],
      [  16,      -1, 'boolean',          :Boolean,       'f',                         false                  ],
      # network address types
      [ 650,      -1, 'cidr',             :CIDR,          '192.168.1.1/32',            '192.168.1.1/32'       ],
      [ 869,      -1, 'inet',             :Inet,          '192.168.1.1/32',            '192.168.1.1/32'       ],
      [ 829,      -1, 'macaddr',          :MACAddr,       '08:00:2b:01:02:03',         '08:00:2b:01:02:03'    ]
    ]

    Values = Types.collect { |t| Value.new(*t) }

    describe Base do
      before(:each) do
        @types = Array.new(Values.size) { |i| Datatype::Base.class_for(Values[i].oid, Values[i].mod) }
      end

      describe '::create' do
        it 'instantiates the proper subclass for the OID' do
          @types.each_with_index do |type, i|
            type::OID.should == Values[i].oid
            type.name.should match(/MosesPG::Datatype::#{Values[i].mpg_class_name.to_s}/)
          end
        end
      end

      describe '#translate' do
        it 'translates the responses to native types' do
          @types.each_with_index do |type, i|
            type.translate(Values[i].literal).should == Values[i].expected
          end
        end
      end
    end

  end

end
