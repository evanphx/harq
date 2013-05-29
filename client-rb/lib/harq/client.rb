require 'socket'
require 'harq/wire'

class Harq
  class Client
    def initialize(host="localhost", port=7621)
      @sock = TCPSocket.new host, port
    end

    def close
      @sock.close
    end

    def subscribe!(dest)
      send_action :type => 1, :payload => dest
    end

    def tap!
      send_action :type => 2
    end

    def durable_subscribe!(dest)
      send_action :type => 3, :payload => dest
    end

    def flush(dest)
      send_action :type => 4, :payload => dest
    end

    def request_ack!
      send_action :type => 5
    end

    def ack(id)
      send_action :type => 6, :id => id
    end

    def request_confirm!
      send_action :type => 7
    end

    def confirm(id)
      send_action :type => 8, :id => id
    end

    def request_stat(dest)
      send_action :type => 9, :payload => dest
    end

    def make_broadcast(dest)
      send_action :type => 10, :payload => dest
    end

    def make_transient(dest)
      send_action :type => 11, :payload => dest
    end

    def make_durable(dest)
      send_action :type => 12, :payload => dest
    end

    def broadcast(dest, payload)
      msg = Wire::Message.new \
              :destination => dest,
              :payload => payload

      send_message msg
    end

    alias_method :queue, :broadcast

    def read
      read_message.payload
    end

    def send_message(msg)
      str = ""
      msg.encode str

      @sock << [str.size].pack("N") << str
    end

    def read_message
      sz = @sock.read(4).unpack("N").first
      Wire::Message.decode @sock.read(sz)
    end

    def ready?
      !IO.select([@sock]).empty?
    end

    def send_action(fields)
      act = Wire::Action.new fields

      str = ""
      act.encode str

      broadcast "+", str
    end

  end
end
