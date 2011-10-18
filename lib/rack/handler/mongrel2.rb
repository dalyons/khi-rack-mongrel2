require 'mongrel2/connection'
require 'mongrel2/thread_pool'
require 'stringio'

module Rack
  module Handler
    class Mongrel2
      
      def self.run(app, options = {})
        
        raise ArgumentError.new('Must specify :recv') if options[:recv].nil?
        raise ArgumentError.new('Must specify :recv') if options[:send].nil?
        raise ArgumentError.new('Must specify :uuid') if options[:uuid].nil?

        conn = ::Mongrel2::Connection.new(options[:uuid], options[:recv], options[:send])

        running = true

        #init a thread pool - ':sized => true' means that once we hit :pool_size active requests,
        #we will block & stop pulling requests off the mongrel socket.
        @thread_pool = ThreadPool.new(options[:pool_size] || 10, :sized => true) if options[:threaded]

        %w(INT TERM KILL).each do | sig |
          Signal.trap(sig) do
            @thread_pool.shutdown if @thread_pool
            running = false
          end
        end
        
        begin 
          while running do
            req = conn.recv
            next if req.nil? || req.disconnect?
            break if !running

            script_name = ENV['RACK_RELATIVE_URL_ROOT'] || req.headers['PATTERN'].split('(', 2).first.gsub(/\/$/, '')
            env = { 'rack.version'        => Rack::VERSION,
                    'rack.url_scheme'     => 'http', # Only HTTP for now
                    'rack.input'          => StringIO.new(req.body),
                    'rack.errors'         => $stderr,
                    'rack.multithread'    => true,
                    'rack.multiprocess'   => true,
                    'rack.run_once'       => false,
                    'mongrel2.pattern'    => req.headers['PATTERN'],
                    'REQUEST_METHOD'      => req.headers['METHOD'],
                    'SCRIPT_NAME'         => script_name,
                    'PATH_INFO'           => req.headers['PATH'].gsub(script_name, ''),
                    'QUERY_STRING'        => req.headers['QUERY'] || '' }

            env['SERVER_NAME'], env['SERVER_PORT'] = req.headers['host'].split(':', 2)
            req.headers.each do |key, val|
              unless key =~ /content_(type|length)/i
                key = "HTTP_#{key.upcase.gsub('-', '_')}"
              end
              env[key] = val
            end

            responder = lambda do |_req, _env|
              status, headers, rack_response = app.call(_env)
              body = ''
              rack_response.each { |b| body << b }
              conn.reply(_req, body, status, headers)
            end

            if options[:threaded]
              @thread_pool.schedule(req.dup, env.dup, &responder)
            else
              responder.call(req, env)
            end
          end
        rescue ::Mongrel2::ConnectionDiedError => e
          conn.close
          exit
          return
        end
      end #def self.run
      
    end #class Mongrel2
  end #module Handler
end #module Rack
