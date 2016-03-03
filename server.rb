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

module SUT
  require_relative 'lib/credentials.rb'
  require_relative 'lib/videos.rb'
  require_relative 'lib/video_event.rb'
  require_relative 'lib/utils.rb'

  class App
    def self.run(cluster)
      session = cluster.connect

      Proc.new do |env|
        begin
          case env['REQUEST_URI']

          ## Basics

          when '/'
            if env['REQUEST_METHOD'] == 'GET'
              begin
                Util.ok_200('Hello World')
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          when '/cassandra'
            if env['REQUEST_METHOD'] == 'GET'
              begin
                session.execute('SELECT NOW() from system.local')
                Util.ok_200
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          ## User Credentials

          when /\/simple\-statements\/credentials\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              future = Credentials.insert_credentials_simple(session, input['email'],
                                                             input['password'] ? input['password'] : input['email'])
              begin
                rows = future.get
                Util.ok_200_json(rows.first)
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            when 'GET'
              email = $1
              future = Credentials.get_credentials_simple(session, email)
              begin
                rows = future.get
                if rows.empty?
                  Util.not_found_404
                else
                  Util.ok_200_json(rows.first)
                end
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          when /\/prepared\-statements\/credentials\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              future = Credentials.insert_credentials_prepared(session, input['email'],
                                                             input['password'] ? input['password'] : input['email'])
              begin
                rows = future.get
                Util.ok_200_json(rows.first)
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            when 'GET'
              email = $1
              future = Credentials.get_credentials_prepared(session, email)
              begin
                rows = future.get
                if rows.empty?
                  Util.not_found_404
                else
                  Util.ok_200_json(rows.first)
                end
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          ## Videos

          when /\/simple\-statements\/videos\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              future = Videos.insert_videos_simple(session, input)
              begin
                rows = future.get
                Util.ok_200_json(rows.first)
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            when 'GET'
              video_id = $1
              future = Videos.get_videos_simple(session, video_id)
              begin
                rows = future.get
                if rows.empty?
                  Util.not_found_404
                else
                  Util.ok_200_json(rows.first)
                end
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          when /\/prepared\-statements\/videos\/?(.*)?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              future = Videos.insert_videos_prepared(session, input)
              begin
                rows = future.get
                Util.ok_200_json(rows.first)
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            when 'GET'
              video_id = $1
              future = Videos.get_videos_prepared(session, video_id)
              begin
                rows = future.get
                if rows.empty?
                  Util.not_found_404
                else
                  Util.ok_200_json(rows.first)
                end
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          ## Video Event

          when /simple\-statements\/video\-events(\/(.*)\/(.*))?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              future = VideoEvent.insert_video_event_simple(session, input)
              begin
                rows = future.get
                Util.ok_200_json(rows.first)
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            when 'GET'
              video_id = $2
              user_id = $3
              future = VideoEvent.get_video_event_simple(session, video_id, user_id)
              begin
                rows = future.get
                if rows.empty?
                  Util.not_found_404
                else
                  Util.ok_200_json(rows.first)
                end
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            else
              Util.not_found_404
            end

          when /prepared\-statements\/video\-events(\/(.*)\/(.*))?/
            case env['REQUEST_METHOD']
            when 'POST'
              input = JSON.parse(env['rack.input'].read)
              future = VideoEvent.insert_video_event_prepared(session, input)
              begin
                rows = future.get
                Util.ok_200_json(rows.first)
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
              end
            when 'GET'
              video_id = $2
              user_id = $3
              future = VideoEvent.get_video_event_prepared(session, video_id, user_id)
              begin
                rows = future.get
                if rows.empty?
                  Util.not_found_404
                else
                  Util.ok_200_json(rows.first)
                end
              rescue => e
                Util.server_error_500("#{e.class.name}: #{e.message}")
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