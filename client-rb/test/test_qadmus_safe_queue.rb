require 'test/unit'
require 'qadmus/safe_queue'

class TestQadmusSafeQueue < Test::Unit::TestCase
  def setup
    @q = Qadmus.new
  end

  def teardown
    @q.close
  end

  def sq
    sq = Qadmus::SafeQueue.new

    begin
      yield sq
    ensure
      sq.close
    end
  end

  def test_subscribe
    msg = nil

    sq do |s|
      s = Qadmus::SafeQueue.new
      s.subscribe("blah") { |m| msg = m }

      @q.broadcast "blah", "abcdef"

      s.poll
    end

    assert_equal "abcdef", msg
  end

  def test_subcribe_that_errors_out
    msg = nil

    sq do |s|
      s = Qadmus::SafeQueue.new

      error = true
      s.subscribe("blah") do |m|
        raise "error" if error
        msg = m
      end

      @q.broadcast "blah", "abcdef"

      assert_raises RuntimeError do
        s.poll
      end

      assert_nil msg
      error = false

      s.poll
    end

    assert_equal "abcdef", msg

  end
end
