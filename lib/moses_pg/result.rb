# encoding: utf-8

module MosesPG

  class Result

    attr_reader :rows, :columns, :tag

    def initialize
      @rows = []
    end

    def columns=(cols)
      @columns = cols
    end

    def <<(row)
      @rows << row
      self
    end

    def finish(tag)
      @tag = tag
      self
    end

    def finished?
      !!@tag
    end

    def result
      self
    end

  end

  class ResultGroup

    attr_reader :result

    def initialize
      @result = [Result.new]
    end

    def columns=(cols)
      current_result { |res| res.columns = cols }
    end

    def <<(cols)
      current_result { |res| res << cols }
      self
    end

    def finish(tag)
      current_result { |res| res.finish(tag) }
      self
    end

    def current_result
      if @result.last.finished?
        res = Result.new
        @result << res
        yield(res)
      else
        yield(@result.last)
      end
    end

  end

end
