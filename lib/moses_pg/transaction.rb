# encoding: utf-8

module MosesPG

  class Transaction

    attr_reader :connection

    def initialize(connection)
      @connection = connection
    end

    def execute(sql)
      @connection.execute(sql, self)
    end

    def prepare(sql, datatypes = nil)
      Statement.prepare(@connection, sql, datatypes, self)
    end

    def commit
      @connection.commit(self)
    end

    def rollback
      @connection.rollback(self)
    end

  end

end
