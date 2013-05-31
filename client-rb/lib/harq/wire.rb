require 'beefcake'

require 'harq/errors'

class Harq
  module Wire
    StatType = 1

    class Message
      include Beefcake::Message

      required :destination, :string, 1
      required :payload, :string, 2

      optional :id, :uint64, 3
      optional :flags, :uint32, 4

      optional :confirm_id, :uint64, 5
      optional :type, :uint32, 6

      def stat?
        type == StatType
      end

      def as_stat
        if stat?
          return Stat.decode(payload)
        end

        nil
      end
    end

    class QueueError
      include Beefcake::Message

      required :queue, :string, 1
      required :error, :string, 2
    end

    class Action
      include Beefcake::Message

      required :type, :int32, 1
      optional :payload, :string, 2
      optional :id, :uint64, 3

      def self.handle(msg)
        act = Action.decode msg.payload

        case act.type
        when 13
          error = QueueError.decode act.payload

          raise Harq::QueueError.new(error.queue, error.error)
        else
          raise Harq::ProtocolError, "Unknown action received: #{act.type}"
        end
      end
    end

    class Stat
      include Beefcake::Message

      required :name, :string, 1
      required :name, :bool, 2
      optional :transient_size, :uint32, 3
      optional :durable_size, :uint32, 4
    end

    class BondRequest
      include Beefcake::Message

      required :queue, :string, 1
      required :destination, :string, 2
    end
  end
end
