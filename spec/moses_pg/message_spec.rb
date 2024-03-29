# encoding: ascii-8bit

#--
# MosesPG -- a Ruby library for accessing PostgreSQL
# Copyright (C) 2012 James Edwin Cain (user: mosespg; domain: jcain.net)
#
# This file is part of the MosesPG library.  This Library is free software; you
# may redistribute it or modify it under the terms of the license contained in
# the file LICENCE.txt. If you did not receive a copy of the license, please
# contact the copyright holder.
#++

require 'moses_pg/message'

module MosesPG

  module Message

    describe AuthenticationOk do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x00")
          @message.should be_instance_of(AuthenticationOk)
        end
      end
    end

    describe AuthenticationKerberosV5 do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x02")
          @message.should be_instance_of(AuthenticationKerberosV5)
        end
      end
    end

    describe AuthenticationCleartextPassword do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x03")
          @message.should be_instance_of(AuthenticationCleartextPassword)
        end
      end
    end

    describe AuthenticationMd5Password do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x05abcd")
          @message.should be_instance_of(AuthenticationMd5Password)
          @message.salt.should == 'abcd'
        end
      end
    end

    describe AuthenticationScmCredential do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x06")
          @message.should be_instance_of(AuthenticationScmCredential)
        end
      end
    end

    describe AuthenticationGss do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x07")
          @message.should be_instance_of(AuthenticationGss)
        end
      end

    end

    describe AuthenticationSspi do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x09")
          @message.should be_instance_of(AuthenticationSspi)
        end
      end
    end

    describe AuthenticationGssContinue do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('R', "\x00\x00\x00\x08this is a test")
          @message.should be_instance_of(AuthenticationGssContinue)
          @message.auth_data.should == 'this is a test'
        end
      end
    end

    describe BackendKeyData do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('K', "\x00\x00\x04\xD2\x00\xBCaN")
          @message.should be_instance_of(BackendKeyData)
          @message.process_id.should == 1234
          @message.secret_key.should == 12345678
        end
      end
    end

    describe Bind do
      describe '::new' do
        describe 'when given an invalid format' do
          it 'raises an error' do
            expect { Bind.new('s1', 'p1', [], 5) }.to raise_error(ArgumentError, /Invalid result format/)
            expect { Bind.new('s1', 'p1', [], [2]) }.to raise_error(ArgumentError, /Invalid result format/)
          end
        end
      end
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Bind.new('stmt1', 'port1', ['this is a test', 'hello'], nil)
          @message.dump.should ==
              "B\x00\x00\x00\x35port1\0stmt1\0\x00\x02\x00\x00\x00\x00\x00\x02\x00\x00\x00\x0ethis is a test\x00\x00\x00\x05hello\x00\x00"
        end
      end
    end

    describe BindComplete do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('2', "")
          @message.should be_instance_of(BindComplete)
        end
      end
    end

    describe CancelRequest do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = CancelRequest.new(123, 456)
          @message.dump.should == "\x00\x00\x00\x10\x04\xD2\x16.\x00\x00\x00{\x00\x00\x01\xC8"
        end
      end
    end

    describe CloseStatement do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = CloseStatement.new('statement1')
          @message.dump.should == "C\x00\x00\x00\x10Sstatement1\0"
        end
      end
    end

    describe ClosePortal do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = ClosePortal.new('portal1')
          @message.dump.should == "C\x00\x00\x00\x0dPportal1\0"
        end
      end
    end

    describe CloseComplete do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('3', "")
          @message.should be_instance_of(CloseComplete)
        end
      end
    end

    describe CommandComplete do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('C', "DELETE 10")
          @message.should be_instance_of(CommandComplete)
          @message.tag.should == 'DELETE 10'
        end
      end
    end

    describe DataRow do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('D',
              "\x00\x05\x00\x00\x00\x04this\x00\x00\x00\x02is\x00\x00\x00\x01a\xFF\xFF\xFF\xFF\x00\x00\x00\x04test")
          @message.should be_instance_of(DataRow)
          @message.row.should == ['this', 'is', 'a', nil, 'test']
        end
      end
    end

    describe DescribeStatement do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = DescribeStatement.new('statement1')
          @message.dump.should == "D\x00\x00\x00\x10Sstatement1\0"
        end
      end
    end

    describe DescribePortal do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = DescribePortal.new('portal1')
          @message.dump.should == "D\x00\x00\x00\x0dPportal1\0"
        end
      end
    end

    describe Execute do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Execute.new('portal1', 100)
          @message.dump.should == "E\x00\x00\x00\x10portal1\0\x00\x00\x00\x64"
        end
      end
    end

    describe EmptyQueryResponse do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('I', '')
          @message.should be_instance_of(EmptyQueryResponse)
        end
      end
    end

    describe Flush do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Flush.instance
          @message.dump.should == "H\x00\x00\x00\x04"
        end
      end
    end

    describe NoData do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('n', '')
          @message.should be_instance_of(NoData)
        end
      end
    end

    describe NoticeResponse do
      describe '::create' do
        it 'creates an instance' do
          message = MosesPG::Message.create('N', "SNOTICE\0MThis is a test\0?Unknown\0")
          message.should be_instance_of(NoticeResponse)
          message.fields.should == {'Severity' => 'NOTICE', 'Message' => 'This is a test', '?' => 'Unknown'}
        end
      end
    end

    describe ParameterDescription do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('t', "\x00\x03\x00\x00\x00\x14\x00\x00\x00\x16\x00\x00\x00\x18")
          @message.should be_instance_of(ParameterDescription)
          @message.oids.should == [20, 22, 24]
        end
      end
    end

    describe ParameterStatus do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('S', "city\0Fort Lauderdale\0")
          @message.should be_instance_of(ParameterStatus)
          @message.name.should == 'city'
          @message.value.should == 'Fort Lauderdale'
        end
      end
    end

    describe Parse do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Parse.new('statement1', 'select * from users where id = $1', [23])
          @message.dump.should == "P\x00\x00\x00\x37statement1\0select * from users where id = $1\0\x00\x01\x00\x00\x00\x17"
        end
      end
    end

    describe ParseComplete do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('1', '')
          @message.should be_instance_of(ParseComplete)
        end
      end
    end

    describe PortalSuspended do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('s', '')
          @message.should be_instance_of(PortalSuspended)
        end
      end
    end

    describe PasswordMessage do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = PasswordMessage.new('this is a test')
          @message.dump.should == "p\x00\x00\x00\x13this is a test\0"
        end
      end
    end

    describe Query do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Query.new('select * from people')
          @message.dump.should == "Q\x00\x00\x00\x19select * from people\0"
        end
      end
    end

    describe ReadyForQuery do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('Z', 'I')
          @message.should be_instance_of(ReadyForQuery)
          @message.status.should == 'I'
        end
      end
    end

    describe RowDescription do
      describe '::create' do
        it 'creates an instance' do
          @message = MosesPG::Message.create('T',
              "\x00\x01name\0\x00\x00\x03\xE7\x00\x01\x00\x00\x00\x17\x00\b\xFF\xFF\xFF\xFF\x00\x00")
          @message.should be_instance_of(RowDescription)
          @message.columns.should == [['name', 999, 1, 23, 8, -1, 0]]
        end
      end
    end

    describe StartupMessage do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = StartupMessage.new('jim', 'inventory')
          @message.dump.should == "\x00\x00\x00\x25\x00\x03\x00\x00user\0jim\0database\0inventory\0\0"
        end
      end
    end

    describe Sync do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Sync.instance
          @message.dump.should == "S\x00\x00\x00\x04"
        end
      end
    end

    describe Terminate do
      describe '#dump' do
        it 'returns the serialized message' do
          @message = Terminate.instance
          @message.dump.should == "X\x00\x00\x00\x04"
        end
      end
    end

  end

end
