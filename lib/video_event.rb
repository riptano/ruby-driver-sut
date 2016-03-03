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
  class VideoEvent
    def self.get_video_event_simple(session, video_id, user_id)
      session.execute_async("SELECT * FROM killrvideo.video_event
                             WHERE videoid=#{Cassandra::Uuid.new(video_id)} AND userid=#{Cassandra::Uuid.new(user_id)}")
    end

    def self.get_video_event_prepared(session, select, video_id, user_id)
      session.execute_async(select, arguments: [Cassandra::Uuid.new(video_id), Cassandra::Uuid.new(user_id)])
    end

    def self.insert_video_event_simple(session, args)
      video_id = Cassandra::Uuid.new(args['videoid'])
      user_id = Cassandra::Uuid.new(args['userid'])
      event_timestamp = Cassandra::TimeUuid.new(args['event_timestamp'])

      session.execute_async("INSERT INTO killrvideo.video_event (videoid, userid, event, event_timestamp, video_timestamp)
                             VALUES (#{video_id},
                             #{user_id},
                             '#{args['event']}',
                             #{event_timestamp},
                             #{args['video_timestamp'].to_i})")
    end

    def self.insert_video_event_prepared(session, insert, args)
      video_id = Cassandra::Uuid.new(args['videoid'])
      user_id = Cassandra::Uuid.new(args['userid'])
      event_timestamp = Cassandra::TimeUuid.new(args['event_timestamp'])

      session.execute_async(insert, arguments: [video_id, user_id, args['event'], event_timestamp,
                                                args['video_timestamp'].to_i])
    end
  end
end