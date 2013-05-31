class Harq
  class Error < RuntimeError; end

  class QueueError < Error
    def initialize(queue, error)
      @queue = queue
      @error = error

      super "#{error} (queue: #{queue})"
    end

    attr_reader :queue, :error
  end

  class ProtocolError < Error; end
end
