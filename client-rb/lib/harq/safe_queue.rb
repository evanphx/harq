require 'harq'

class Harq
  # A message handler that collects subscriptions and invokes
  # handlers for each when they arrive.
  #
  # If the handler raises an exception, 
  # the message will not be ack'd and the broker will requeue it.
  #
  class SafeQueue
    def initialize(host="localhost", port=7621)
      @client = Client.new host, port
      @client.request_ack!

      @handlers = {}
    end

    def close
      @client.close
    end

    def subscribe(name, &blk)
      @client.subscribe! name
      @handlers[name] = blk
    end

    def poll
      if @client.ready?
        process_one
      end
    end

    def process_one
      msg = @client.read_message
      if blk = @handlers[msg.destination]
        blk.call msg.payload
        @client.ack msg.id
      end
    end

    def process
      process_one while true
    end
  end
end
