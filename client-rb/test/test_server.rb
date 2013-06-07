require 'test/unit'
require 'harq/client'
require 'fileutils'

class TestServer < Test::Unit::TestCase
  Q = "&rubytest"
  P = "payload"

  def setup
    @clients = []
  end

  def teardown
    @clients.each do |c|
      c.close rescue nil
    end
  end

  def connect
    c = Harq::Client.new
    @clients << c
    c
  end

  def assert_queue_size(b, size)
    b.request_stat Q

    msg = b.read_message
    assert msg.stat?

    s = msg.as_stat

    assert_equal size, s.size
  end

  def test_queue
    a = connect

    a.make_ephemeral Q

    a.queue Q, P

    b = connect

    b.subscribe! Q

    assert_equal P, b.read
  end

  def test_messages_held_by_inflight
    c = connect
    c.make_ephemeral Q
    c.queue Q, "p1"
    c.queue Q, "p2"

    c.request_ack!

    c.subscribe! Q

    m = c.read_message

    assert_equal "p1", m.payload

    assert !c.ready?(1)

    assert_queue_size c, 1

    c.ack m.id

    assert c.ready?(1)
    m = c.read_message

    assert_equal "p2", m.payload
  end

end
