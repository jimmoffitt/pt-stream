'''
This streaming HTTP module is based on the gnip-stream project (https://github.com/rweald/gnip-stream).
Code from that project was compiled within this one file, along with a few
name-space refactoring.
'''

require 'eventmachine'
require 'em-http-request'

module StreamDelegate
    #Magic Method that will delegate all
    # method calls to underlying stream if possible
    def method_missing(m, *args, &block)
        if @stream.respond_to?(m)
            @stream.send(m, *args, &block)
        else
            super(m, *args, &block)
        end
    end
end

class JsonStream
    include StreamDelegate
    def initialize(url, headers={})
        json_processor = DataBuffer.new(Regexp.new(/^(\{.*\})\s\s(.*)/))
        @stream = Stream.new(url, json_processor, headers)
    end
end

class XmlStream
    include StreamDelegate
    def initialize(url, headers={})
        xml_processor = DataBuffer.new(Regexp.new(/^(\<entry.*?\<\/entry\>)(.*)/m))
        @stream = Stream.new(url, xml_processor, headers)
    end
end


class ErrorReconnect
    def initialize(source_class, method_name)
        @source_class = source_class
        @method_name = method_name
        @reconnect_attempts = 0
    end

    def attempt_to_reconnect(error_message)
        @error_message = error_message
        if @reconnect_attempts < 5
            @reconnect_attempts +=1
            sleep(2)
            @source_class.send(@method_name)
        else
            reconnect_failed_raise_error
        end
    end

    def reconnect_failed_raise_error
        raise @error_message
    end

end


class DataBuffer
    attr_accessor :pattern
    def initialize(pattern)
        @pattern = pattern
        @buffer = ""
    end

    def process(chunk)
        #print "."   #Heart tesing
        @buffer.concat(chunk)
    end

    def complete_entries
        entries = []
        while (match_data = @buffer.match(@pattern))
            entries << match_data.captures[0]
            @buffer = match_data.captures[1]
        end
        entries
    end
end

class Stream

    EventMachine.threadpool_size = 3

    attr_accessor :headers, :options, :url, :username, :password

    def initialize(url, processor, headers={})
        @url = url
        @headers = headers
        @processor = processor
    end

    def on_message(&block)
        @on_message = block
    end

    def on_connection_close(&block)
        @on_connection_close = block
    end

    def on_error(&block)
        @on_error = block
    end

    def connect
        EM.run do
            http = EM::HttpRequest.new(@url, :inactivity_timeout => 2**16, :connection_timeout => 2**16).get(:head => @headers)
            http.stream { |chunk| process_chunk(chunk) }
            http.callback {
                handle_connection_close(http)
                EM.stop
            }
            http.errback {
                handle_error(http)
                EM.stop
            }
        end
    end

    def process_chunk(chunk)
        @processor.process(chunk)
        @processor.complete_entries.each do |entry|
            EM.defer { @on_message.call(entry) }
        end
    end

    def handle_error(http_connection)
        @on_error.call(http_connection)
    end

    def handle_connection_close(http_connection)
        @on_connection_close.call(http_connection)
    end

end

class PowertrackStream
    def initialize(url, username, password)
        @stream = JsonStream.new(url, {"authorization" => [username, password], 'accept-encoding' => 'gzip, compressed'})

        @error_handler = ErrorReconnect.new(self, :consume)
        @connection_close_handler = ErrorReconnect.new(self, :consume)
        configure_handlers
    end

    def configure_handlers
        @stream.on_error { |error| @error_handler.attempt_to_reconnect("Gnip Connection Error. Reason was: #{error.inspect}") }
        @stream.on_connection_close { @connection_close_handler.attempt_to_reconnect("Gnip Connection Closed") }
    end

    def consume(&block)
        @client_callback = block if block
        @stream.on_message(&@client_callback)
        @stream.connect
    end
end


class EDCStream
    def initialize(url, username, password)
        @stream = XmlStream.new(url, "authorization" => [username, password])
        @error_handler = ErrorReconnect.new(self, :consume)
        @connection_close_handler = ErrorReconnect.new(self, :consume)
        configure_handlers
    end

    def configure_handlers
        @stream.on_error { |error| @error_handler.attempt_to_reconnect("Gnip Connection Error. Reason was: #{error.inspect}") }
        @stream.on_connection_close { @connection_close_handler.attempt_to_reconnect("Gnip Connection Closed") }
    end

    def consume(&block)
        @client_callback = block if block
        @stream.on_message(&@client_callback)
        @stream.connect
    end
end
