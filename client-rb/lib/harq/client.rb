require 'socket'
require 'harq/wire'

class Harq
  class Client

    DEFAULT_PORT = 7621

    def initialize(host="localhost", port=DEFAULT_PORT)
      @sock = TCPSocket.new host, port
    end

    def to_io
      @sock
    end

    def close
      @sock.close
    end

    def configure(hsh)
      cfg = Wire::ConnectionConfigure.new hsh
      str = ""
      cfg.encode(str)
      send_action :type => 2, :payload => str
    end

    def tap!
      configure :tap => true
    end

    def request_confirm!
      configure :confirm => true
    end

    def request_ack!
      configure :ack => true
    end

    def inflight_max=(val)
      configure :inflight => val.to_i
    end

    def subscribe!(dest)
      send_action :type => 1, :payload => dest
    end

    def flush(dest)
      send_action :type => 4, :payload => dest
    end

    def ack(id)
      send_action :type => 6, :id => id
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

    def make_ephemeral(dest)
      send_action :type => 15, :payload => dest
    end

    def request_bond(queue, dest)
      br = Wire::BondRequest.new :queue => queue, :destination => dest

      str = ""
      br.encode str

      send_action :type => 14, :payload => str
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
      while true
        sz = @sock.read(4).unpack("N").first
        msg = Wire::Message.decode @sock.read(sz)

        if msg.destination == "+"
          Wire::Action.handle msg
        else
          return msg
        end
      end
    end

    def ready?(timeout=0)
      !!IO.select([@sock], nil, nil, timeout)
    end

    def send_action(fields)
      act = Wire::Action.new fields

      str = ""
      act.encode str

      broadcast "+", str
    end

  end
end
