require 'test/unit'
require 'harq'

class TestHarq < Test::Unit::TestCase
  def setup
    @s = Harq.new
    @s.subscribe "/test"

    @q = Harq.new
  end

  def teardown
    @s.close
    @q.close
  end

  def payload
    @s.read
  end

  def test_broadcast
    @q.broadcast "/test", "abcdef"
    assert_equal "abcdef", payload
  end

  def test_queue_when_connected
    @q.queue "/test", "abcdef"
    assert_equal "abcdef", payload
  end

  def test_queue_when_disconnected
    @s.close

    @q.queue "/test", "abcdef"

    @s = Harq.new
    @s.subscribe "/test"
    assert_equal "abcdef", payload
  end
end
