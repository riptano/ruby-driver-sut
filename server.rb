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

module SUT
  require_relative 'lib/utils.rb'
  require_relative 'lib/users.rb'

  class App
    def self.run(cluster)
      session = cluster.connect

      Proc.new do |env|
        begin
          case env['REQUEST_URI']

            when '/'
              if env['REQUEST_METHOD'] == 'GET'
                Util.ok_200('Hello World')
              else
                Util.not_found_404
              end

            when '/cassandra'
              if env['REQUEST_METHOD'] == 'GET'
                session.execute('SELECT NOW() from system.local')
                Util.no_content_204
              else
                Util.not_found_404
              end

            when /\/simple\-statements\/users\/(\d+)(\/\d+)?/
              range_start = Integer($1)
              range_end   = range_start + ($2 ? Integer($2[1..-1]) : 1)

              case env['REQUEST_METHOD']
                when 'POST'
                  futures = (range_start...range_end).map do |id|
                    Users.insert_user_simple(session, id)
                  end
                  begin
                    Cassandra::Future.all(futures).get
                    Util.ok_200
                  rescue Cassandra::Errors::InvalidError => e
                    Util.conflict_409(e.message)
                  end
                when 'GET'
                  futures = (range_start...range_end).map do |id|
                      Users.get_user_simple(session, id)
                    end
                  begin
                    usernames = Cassandra::Future.all(futures).get
                    Util.ok_200(usernames.join(','))
                  rescue Cassandra::Errors::InvalidError => e
                    Util.conflict_409(e.message)
                  end
              else
                Util.not_found_404
              end

            when /\/prepared\-statements\/users\/(\d+)(\/\d+)?/
              range_start = Integer($1)
              range_end   = range_start + ($2 ? Integer($2[1..-1]) : 1)

              case env['REQUEST_METHOD']
                when 'POST'
                  futures = (range_start...range_end).map do |id|
                    Users.insert_user_prepared(session, id)
                  end
                  begin
                    Cassandra::Future.all(futures).get
                    Util.ok_200
                  rescue Cassandra::Errors::InvalidError => e
                    Util.conflict_409(e.message)
                  end
                when 'GET'
                  futures   = (range_start...range_end).map do |id|
                    Users.get_user_prepared(session, id)
                  end
                  begin
                    usernames = Cassandra::Future.all(futures).get
                    Util.ok_200(usernames.join(','))
                  rescue Cassandra::Errors::InvalidError => e
                    Util.conflict_409(e.message)
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