require 'test/unit'
require 'harq/client'
require 'fileutils'

class TestServer < Test::Unit::TestCase
  EXEC = File.expand_path "../../../harq", __FILE__
  TEST_DB = File.expand_path "../test.db", __FILE__
  TEST_PORT = 12032

  DEST = "test"
  PAYLOAD = "payload1234"

  SHOW_DEBUG = ENV['HARQ_DEBUG']

  def stop!
    @clients.each { |c| c.close rescue nil }
    Process.kill "INT", @pid
    @io.close
  end

  def start!
    if SHOW_DEBUG
      @io = IO.popen "#{EXEC} -p #{TEST_PORT} -d #{TEST_DB}", "w"
    else
      @io = IO.popen "#{EXEC} -p #{TEST_PORT} -d #{TEST_DB} > /dev/null", "w"
    end

    @pid = @io.pid

    sleep 0.25
    @clients = []
  end

  def restart!
    start!
    stop!
  end

  def setup
    start!
  end

  def teardown
    stop!
  ensure
    FileUtils.rm_rf TEST_DB
  end

  def assert_clean
    stop!
    str = `#{EXEC} fsck #{TEST_DB} 2>&1`
    assert_equal 0, $?.exitstatus, str
    start!
  end

  def assert_queue_size(b, size)
    b.request_stat DEST

    msg = b.read_message
    assert msg.stat?

    s = msg.as_stat

    assert_equal size, s.transient_size + s.durable_size
  end

  def connect
    c = Harq::Client.new "localhost", TEST_PORT
    @clients << c
    c
  end

  def test_queue
    a = connect

    a.make_transient DEST

    a.queue DEST, PAYLOAD

    b = connect

    b.subscribe! DEST

    assert_equal PAYLOAD, b.read
  end

  def test_transient_queue_lost_on_restart
    a = connect

    a.make_transient DEST
    a.queue DEST, PAYLOAD

    restart!

    b = connect

    b.request_stat DEST

    msg = b.read_message
    assert msg.stat?

    assert_equal 0, msg.as_stat.transient_size
  end

  def test_durable_messages_properly_resaved_on_failed_ack
    c = connect
    c.make_durable DEST
    c.queue DEST, "p1"
    c.queue DEST, "p2"

    c.request_ack!

    c.subscribe! DEST

    c.close

    sleep 1
    assert_clean

    c = connect

    assert_queue_size c, 2
  end

end
