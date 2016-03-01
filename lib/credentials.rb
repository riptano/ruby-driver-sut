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
  class Credentials
    def self.get_credentials_simple(session, email)
      session.execute_async("SELECT * FROM killrvideo.user_credentials WHERE email='#{email}'")
    end

    def self.get_credentials_prepared(session, email)
      select = session.prepare('SELECT * FROM killrvideo.user_credentials WHERE email = ?')
      session.execute_async(select, arguments: [email])
    end

    def self.insert_credentials_simple(session, email, password)
      generator = Cassandra::Uuid::Generator.new
      id = generator.uuid
      session.execute_async("INSERT INTO killrvideo.user_credentials (email, password, userid) VALUES ('#{email}', '#{password}', #{id})").get
      session.execute_async("SELECT * FROM killrvideo.user_credentials WHERE email='#{email}'")
    end

    def self.insert_credentials_prepared(session, email, password)
      generator = Cassandra::Uuid::Generator.new
      id = generator.uuid
      insert = session.prepare('INSERT INTO killrvideo.user_credentials (email, password, userid) VALUES (?, ?, ?)')
      session.execute_async(insert, arguments: [email, password, id])

      select = session.prepare('SELECT * FROM killrvideo.user_credentials WHERE email = ?')
      session.execute_async(select, arguments: [email])
    end
  end
end