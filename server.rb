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

      while true do
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
            client.metrics({ name + '.thoughput' => throughput[timestamp] }, timestamp)

            i -= 1
          end
        end

        sleep(interval)
      end
    end
  end

  class App
    def self.run(cluster)
      session = cluster.connect
      select_credentials = session.prepare('SELECT * FROM killrvideo.user_credentials WHERE email = ?')
      insert_credentials = session.prepare('INSERT INTO killrvideo.user_credentials
                                            (email, password, userid) VALUES (?, ?, ?)')
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
      select_video_event = session.prepare('SELECT * FROM killrvideo.video_event WHERE videoid = ? AND userid = ?')
      insert_video_event = session.prepare('INSERT INTO killrvideo.video_event
                                            (videoid, userid, event, event_timestamp, video_timestamp)
                                            VALUES (?, ?, ?, ?, ?)')

      metrics = Metrics.new
      client = GraphiteAPI.new(graphite: '104.197.106.246', prefix: ['sut', 'ruby-driver', '2_1_5'])
      thread = Thread.new { GraphiteThread.new.run(client, metrics, 10) }

      Proc.new do |env|
        begin
          case env['REQUEST_URI']

          ## Basics

          when '/'
            if env['REQUEST_METHOD'] == 'GET'
              Util.ok_200('Hello World')
            else
              Util.not_found_404
            end

          when '/cassandra'
            if env['REQUEST_METHOD'] == 'GET'
              session.execute('SELECT NOW() from system.local')
              Util.ok_200
            else
              Util.not_found_404
            end

          ## User Credentials

          when /\/simple\-statements\/credentials\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              start_time = Time.new
              future = Credentials.insert_credentials_simple(session, input['email'],
                                                             input['password'] ? input['password'] : input['email'])
              metrics.record_metric('simple.insert.user_credentials', future, start_time)
              Util.ok_200_json
            when 'GET'
              email = $1
              start_time = Time.new
              future = Credentials.get_credentials_simple(session, email)
              metrics.record_metric('simple.select.user_credentials', future, start_time)
              rows = future.get
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          when /\/prepared\-statements\/credentials\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              start_time = Time.new
              future = Credentials.insert_credentials_prepared(session, insert_credentials, input['email'],
                                                             input['password'] ? input['password'] : input['email'])
              metrics.record_metric('prepared.insert.user_credentials', future, start_time)
              future.get
              Util.ok_200_json
            when 'GET'
              email = $1
              start_time = Time.new
              future = Credentials.get_credentials_prepared(session, select_credentials, email)
              metrics.record_metric('prepared.select.user_credentials', future, start_time)
              rows = future.get
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          ## Videos

          when /\/simple\-statements\/videos\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              start_time = Time.new
              future = Videos.insert_videos_simple(session, input)
              metrics.record_metric('simple.insert.videos', future, start_time)
              future.get
              Util.ok_200_json
            when 'GET'
              video_id = $1
              start_time = Time.new
              future = Videos.get_videos_simple(session, video_id)
              metrics.record_metric('simple.select.videos', future, start_time)
              rows = future.get
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          when /\/prepared\-statements\/videos\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              start_time = Time.new
              future = Videos.insert_videos_prepared(session, insert_videos, input)
              metrics.record_metric('prepared.insert.videos', future, start_time)
              future.get
              Util.ok_200_json
            when 'GET'
              video_id = $1
              start_time = Time.new
              future = Videos.get_videos_prepared(session, select_videos, video_id)
              metrics.record_metric('prepared.select.videos', future, start_time)
              rows = future.get
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          ## Video Event

          when /simple\-statements\/video\-events(\/(.*)\/(.*))?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              start_time = Time.new
              future = VideoEvent.insert_video_event_simple(session, input)
              metrics.record_metric('simple.insert.video_event', future, start_time)
              future.get
              Util.ok_200_json
            when 'GET'
              video_id = $2
              user_id = $3
              start_time = Time.new
              future = VideoEvent.get_video_event_simple(session, video_id, user_id)
              metrics.record_metric('simple.select.video_event', future, start_time)
              rows = future.get
              if rows.empty?
                Util.not_found_404
              else
                Util.ok_200_json(rows.first)
              end
            else
              Util.not_found_404
            end

          when /prepared\-statements\/video\-events(\/(.*)\/(.*))?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              start_time = Time.new
              future = VideoEvent.insert_video_event_prepared(session, insert_video_event, input)
              metrics.record_metric('prepared.insert.video_event', future, start_time)
              future.get
              Util.ok_200_json
            when 'GET'
              video_id = $2
              user_id = $3
              start_time = Time.new
              future = VideoEvent.get_video_event_prepared(session, select_video_event, video_id, user_id)
              metrics.record_metric('prepared.select.video_event', future, start_time)
              rows = future.get
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
          Util.server_error_500("#{e.class.name}: #{e.message}")
        end
      end
    end
  end

  TorqueBox::Web.run(
    rack_app: App.run(Cassandra.cluster),
    host: '0.0.0.0',
    port: 8080
  ).run_from_cli
end