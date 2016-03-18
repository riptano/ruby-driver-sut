# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'thread'
require 'cassandra'
require 'torquebox/web'
require 'json'
require 'descriptive_statistics'
require 'graphite-api'

module SUT
  require_relative 'lib/credentials.rb'
  require_relative 'lib/videos.rb'
  require_relative 'lib/video_event.rb'
  require_relative 'lib/utils.rb'
  require_relative 'lib/metrics.rb'

  class GraphiteThread
    def run(client, metrics, interval)
      percentiles = [50, 60, 70, 80, 90, 95, 99, 99.9]

      loop do
        metrics.statistics.each_pair do |name, stat|
          curr_time = Time.new.to_i
          latency = stat.latency
          throughput = stat.throughput

          i = interval
          while i > 0
            timestamp = curr_time - i

            # Latency
            percentiles.each do |percentile|
              if percentile == 99
                client.metrics({ name + '.latency.p990' => latency[timestamp].percentile(percentile) }, timestamp)
              elsif percentile == 99.9
                client.metrics({ name + '.latency.p999' => latency[timestamp].percentile(percentile) }, timestamp)
              else
                client.metrics({ name + '.latency.p' + percentile.to_s => latency[timestamp].percentile(percentile) },
                               timestamp)
              end
            end
            client.metrics({ name + '.latency.avg' => latency[timestamp].mean }, timestamp)
            client.metrics({ name + '.latency.max' => latency[timestamp].max }, timestamp)

            # Throughput
            client.metrics({ name + '.throughput' => throughput[timestamp] }, timestamp)

            i -= 1
          end

          # Num Errors
          client.metrics(name + '.num_errors' => stat.num_errors)
        end

        sleep(interval)
      end
    end
  end

  class App
    def self.run(session, experiment, statement, metrics, multiplier)
      if statement == 'prepared'
        if experiment == 'user_credentials'
          select_credentials = session.prepare('SELECT * FROM killrvideo.user_credentials WHERE email = ?')
          insert_credentials = session.prepare('INSERT INTO killrvideo.user_credentials
                                                (email, password, userid) VALUES (?, ?, ?)')
        elsif experiment == 'videos'
          select_videos = session.prepare('SELECT * FROM killrvideo.videos WHERE videoid = ?')
          insert_videos = session.prepare('INSERT INTO killrvideo.videos (
                                           videoid,
                                           userid,
                                           name,
                                           description,
                                           location,
                                           location_type,
                                           preview_thumbnails,
                                           tags,
                                           added_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
        elsif experiment == 'video_event'
          select_video_event = session.prepare('SELECT * FROM killrvideo.video_event WHERE videoid = ? AND userid = ?')
          insert_video_event = session.prepare('INSERT INTO killrvideo.video_event
                                                (videoid, userid, event, event_timestamp, video_timestamp)
                                                VALUES (?, ?, ?, ?, ?)')
        end
      end

      simple_credential_uri = /\/simple\-statements\/credentials\/?(?<email>.*)?/
      prepared_credential_uri = /\/prepared\-statements\/credentials\/?(?<email>.*)?/
      simple_videos_uri = /\/simple\-statements\/videos\/?(?<video_id>.*)?/
      prepared_videos_uri = /\/prepared\-statements\/videos\/?(?<video_id>.*)?/
      simple_video_event_uri = /simple\-statements\/video\-events(\/(?<video_id>.*)\/(?<user_id>.*))?/
      prepared_video_event_uri = /prepared\-statements\/video\-events(\/(?<video_id>.*)\/(?<user_id>.*))?/

      Proc.new do |env|
        begin
          uri = env['REQUEST_URI']

          ## Basics

          if uri == '/'
            if env['REQUEST_METHOD'] == 'GET'
              Util.ok_200('Hello World')
            else
              Util.not_found_404
            end

          elsif uri == '/cassandra'
            if env['REQUEST_METHOD'] == 'GET'
              session.execute('SELECT NOW() from system.local')
              Util.ok_200
            else
              Util.not_found_404
            end

          ## User Credentials

          elsif (matches = simple_credential_uri.match(uri))
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Credentials.insert_credentials_simple(session, input['email'],
                                                               input['password'] ? input['password'] : input['email'])
                metrics.record_metric('simple.insert.user_credentials', future, start_time)
                futures << future
              end
              Cassandra::Future.all(futures).get
              Util.ok_200_json
            when 'GET'
              start_time = Time.new
              futures = []
              multiplier.times do
                future = Credentials.get_credentials_simple(session, matches['email'])
                metrics.record_metric('simple.select.user_credentials', future, start_time)
                futures << future
              end
              rows = Cassandra::Future.all(futures).get.first
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          elsif (matches = prepared_credential_uri.match(uri))
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Credentials.insert_credentials_prepared(session, insert_credentials, input['email'],
                                                               input['password'] ? input['password'] : input['email'])
                metrics.record_metric('prepared.insert.user_credentials', future, start_time)
                futures << future
              end
              Cassandra::Future.all(futures).get
              Util.ok_200_json
            when 'GET'
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Credentials.get_credentials_prepared(session, select_credentials, matches['email'])
                metrics.record_metric('prepared.select.user_credentials', future, start_time)
                futures << future
              end
              rows = Cassandra::Future.all(futures).get.first
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          ## Videos

          elsif (matches = simple_videos_uri.match(uri))
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Videos.insert_videos_simple(session, input)
                metrics.record_metric('simple.insert.videos', future, start_time)
                futures << future
              end
              Cassandra::Future.all(futures).get
              Util.ok_200_json
            when 'GET'
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Videos.get_videos_simple(session, matches['video_id'])
                metrics.record_metric('simple.select.videos', future, start_time)
                futures << future
              end
              rows = Cassandra::Future.all(futures).get.first
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          elsif (matches = prepared_videos_uri.match(uri))
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Videos.insert_videos_prepared(session, insert_videos, input)
                metrics.record_metric('prepared.insert.videos', future, start_time)
                futures << future
              end
              Cassandra::Future.all(futures).get
              Util.ok_200_json
            when 'GET'
              futures = []
              multiplier.times do
                start_time = Time.new
                future = Videos.get_videos_prepared(session, select_videos, matches['video_id'])
                metrics.record_metric('prepared.select.videos', future, start_time)
                futures << future
              end
              rows = Cassandra::Future.all(futures).get.first
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          ## Video Event

          elsif (matches = simple_video_event_uri.match(uri))
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              futures = []
              multiplier.times do
                start_time = Time.new
                future = VideoEvent.insert_video_event_simple(session, input)
                metrics.record_metric('simple.insert.video_event', future, start_time)
                futures << future
              end
              Cassandra::Future.all(futures).get
              Util.ok_200_json
            when 'GET'
              futures = []
              multiplier.times do
                start_time = Time.new
                future = VideoEvent.get_video_event_simple(session, matches['video_id'], matches['user_id'])
                metrics.record_metric('simple.select.video_event', future, start_time)
                futures << future
              end
              rows = Cassandra::Future.all(futures).get.first
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          elsif (matches = prepared_video_event_uri.match(uri))
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              futures = []
              multiplier.times do
                start_time = Time.new
                future = VideoEvent.insert_video_event_prepared(session, insert_video_event, input)
                metrics.record_metric('prepared.insert.video_event', future, start_time)
                futures << future
              end
              Cassandra::Future.all(futures).get
              Util.ok_200_json
            when 'GET'
              futures = []
              multiplier.times do
                start_time = Time.new
                future = VideoEvent.get_video_event_prepared(session, select_video_event,
                                                             matches['video_id'], matches['user_id'])
                metrics.record_metric('prepared.select.video_event', future, start_time)
                futures << future
              end
              rows = Cassandra::Future.all(futures).get.first
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          else
            Util.not_found_404
          end
        rescue => e
          puts "#{e.class.name}: #{e.message}. #{e.backtrace.inspect}"
          Util.server_error_500("#{e.class.name}: #{e.message}. #{e.backtrace.inspect}")
        end
      end
    end
  end

  # Parse command-line args
  OPTIONS = {}
  OptionParser.new do |opts|
    opts.banner = "Usage: server.rb -H [hosts] -V [version] -E [experiment] -S [statement] -G [graphite] -F [frequency] -M [multiplier]"

    opts.on("-H HOSTS", "--hosts", String, "A host to connect to") do |v|
      OPTIONS[:hosts] = v
    end
    opts.on("-V VERSION", "--version", String, "Driver version") do |v|
      OPTIONS[:version] = v
    end
    opts.on("-E EXPERIMENT", "--experiment", String, "Experiment name to be run") do |v|
      OPTIONS[:experiment] = v
    end
    opts.on("-S STATEMENT", "--statement", String, "The statement type") do |v|
      OPTIONS[:statement] = v
    end
    opts.on("-G GRAPHITE", "--graphite", String, "The Graphite server's IP") do |v|
      OPTIONS[:graphite] = v
    end
    opts.on("-F FREQUENCY", "--frequency", Integer, "Frequency of reporting metrics to Graphite") do |v|
      OPTIONS[:frequency] = v
    end
    opts.on("-M MULTIPLIER", "--multiplier", Integer, "Multipler http-request->cql query") do |v|
      OPTIONS[:multiplier] = v
    end
    opts.on_tail("-h", "--help", "Show this message") do
      puts opts
      exit
    end
  end.parse!

  # Validate command-line args
  raise OptionParser::MissingArgument, "Must provide a host to connect to '-H'" if OPTIONS[:hosts].nil?
  raise OptionParser::MissingArgument, "Must provide driver version '-V'" if OPTIONS[:version].nil?
  raise OptionParser::MissingArgument, "Must provide an experiment '-E'" if OPTIONS[:experiment].nil?
  raise OptionParser::MissingArgument, "Must provide a statement type '-S'" if OPTIONS[:statement].nil?
  raise OptionParser::MissingArgument, "Must provide Graphite hosts' IP '-G'" if OPTIONS[:graphite].nil?
  raise OptionParser::MissingArgument, "Must provide Graphite reporting frequency '-F'" if OPTIONS[:frequency].nil?
  raise OptionParser::MissingArgument, "Must provide multiplier '-M'" if OPTIONS[:multiplier].nil?

  # Setup metrics and cluster
  metrics = Metrics.new(OPTIONS[:experiment], OPTIONS[:statement])
  cluster = Cassandra.cluster(hosts: [OPTIONS[:hosts]])
  session = cluster.connect

  client = GraphiteAPI.new(graphite: OPTIONS[:graphite], prefix: ['sut', 'ruby-driver',
                                                                  OPTIONS[:version].gsub(/\./, '.' => '_')])
  thread = Thread.new { GraphiteThread.new.run(client, metrics, OPTIONS[:frequency]) }
  thread.abort_on_exception = true

  # Export metrics as JSON on exit
  at_exit do
    export_metrics = Hash.new

    metrics.statistics.each_pair do |name, stat|
      export_metrics[name] = Hash.new
      export_metrics[name]['latency'] = stat.latency
      export_metrics[name]['throughput'] = stat.throughput
      export_metrics[name]['num_errors'] = stat.num_errors
      export_metrics[name]['errors'] = stat.errors
    end

    File.write('/mnt/logs/metrics.json', export_metrics.to_json)
    cluster.close
    Thread.kill(thread)
  end

  TorqueBox::Web.run(
    rack_app: App.run(session, OPTIONS[:experiment], OPTIONS[:statement], metrics, OPTIONS[:multiplier]),
    host: '0.0.0.0',
    port: 8080
  ).run_from_cli
end
