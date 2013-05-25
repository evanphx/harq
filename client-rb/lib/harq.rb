require 'harq/client'

class Harq
  VERSION = '1.0.0'

  def initialize(host="localhost", port=7621)
    @client = Client.new host, port
    @ack = false
  end

  def close
    @client.close
  end

  def request_ack!
    @ack = true
    @client.request_ack!
  end

  def read
    msg = @client.read_message

    if @ack
      @client.ack msg.id
    end

    msg.payload
  end

  def subscribe(dest)
    @client.subscribe! dest
  end

  def broadcast(dest, payload)
    @client.broadcast dest, payload
  end

  def queue(dest, payload)
    @client.queue dest, payload
  end
end
