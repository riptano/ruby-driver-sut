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

module SUT
  class Users
    def self.get_user_simple(session, id)
      username = "user-#{id}"
      session.execute_async(
          "SELECT username, firstname, lastname, password, email, created_date FROM videodb.users WHERE username = ?", arguments: [username]
      ).then do |rows|
        row = rows.first
        row && row['username']
        end
    end

    def self.get_user_prepared(session, id)
      statement = session.prepare("SELECT username, firstname, lastname, password, email, created_date FROM videodb.users WHERE username = ?")
      username = "user-#{id}"
      session.execute_async(
          statement, arguments: [username]
      ).then do |rows|
        row = rows.first
        row && row['username']
      end
    end

    def self.insert_user_simple(session, id)
      username = "user-#{id}"
      session.execute_async(
          "INSERT INTO videodb.users (username, firstname, lastname, password, email, created_date) VALUES (?, ?, ?, ?, ?, ?)", arguments: [
          username, "First Name #{id}", "Last Name #{id}", 'password',
          ["#{username}@relational.com", "#{username}@nosql.com"], Time.now
      ])
    end

    def self.insert_user_prepared(session, id)
      statement = session.prepare("INSERT INTO videodb.users (username, firstname, lastname, password, email, created_date) VALUES (?, ?, ?, ?, ?, ?)")
      username = "user-#{id}"
      session.execute_async(
          statement, arguments: [
          username, "First Name #{id}", "Last Name #{id}", 'password',
          ["#{username}@relational.com", "#{username}@nosql.com"], Time.now
      ])
    end
  end
end