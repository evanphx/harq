require 'test/unit'
require 'harq/client'
require 'fileutils'

class TestServer < Test::Unit::TestCase
  EXEC = File.expand_path "../../../harq", __FILE__
  TEST_DB = File.expand_path "../test.db"
  TEST_PORT = 12032

  DEST = "test"
  PAYLOAD = "payload1234"

  def setup
    @io = IO.popen "#{EXEC} -p #{TEST_PORT} -d #{TEST_DB}"
    sleep 0.25
    @clients = []
  end

  def teardown
    @clients.each { |c| c.close }
    Process.kill "INT", @io.pid
    @io.close
    FileUtils.rm_rf TEST_DB
  end

  def restart!
    @clients.each { |c| c.close }
    Process.kill "INT", @io.pid
    @io.close

    @io = IO.popen "#{EXEC} -p #{TEST_PORT} -d #{TEST_DB}"
    sleep 0.25
    @clients = []
  end

  def connect
    c = Harq::Client.new "localhost", TEST_PORT
    @clients << c
    c
  end

  def test_broadcast_to_no_queue
    a = connect
    b = connect

    b.subscribe! DEST

    a.broadcast DEST, PAYLOAD

    assert_equal PAYLOAD, b.read
  end

  def test_queue
    a = connect

    a.queue DEST, PAYLOAD

    b = connect

    b.subscribe! DEST

    assert_equal PAYLOAD, b.read
  end

  def test_transient_queue_lost_on_restart
    a = connect

    a.queue DEST, PAYLOAD

    restart!

    b = connect

    b.request_stat DEST

    msg = b.read_message
    assert msg.stat?

    assert_equal 0, msg.as_stat.transient_size
  end

end
