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
  class Videos
    def self.get_videos_simple(session, video_id)
      session.execute_async("SELECT * FROM killrvideo.videos WHERE videoid=#{Cassandra::Uuid.new(video_id)}")
    end

    def self.get_videos_prepared(session, select, video_id)
      session.execute_async(select, arguments: [Cassandra::Uuid.new(video_id)])
    end

    def self.insert_videos_simple(session, args)
      args_map = Hash.new
      args_map[:video_id] = Cassandra::Uuid.new(args['videoid'])
      args_map[:user_id] = Cassandra::Uuid.new(args['userid'])
      args_map[:name] = args['name']
      args_map[:description] = args['description']
      args_map[:location] = args['location']
      args_map[:location_type] = args['location_type'].to_i
      args_map[:preview_thumbnails] = args['preview_thumbnails'].to_h
      args_map[:tags] = Set.new(args['tags'])
      args_map[:added_date] = Time.parse(args['added_date'])

      statement = Cassandra::Statements::Simple.new('INSERT INTO killrvideo.videos (
                                                     videoid,
                                                     userid,
                                                     name,
                                                     description,
                                                     location,
                                                     location_type,
                                                     preview_thumbnails,
                                                     tags,
                                                     added_date) VALUES (
                                                     :video_id,
                                                     :user_id,
                                                     :name,
                                                     :description,
                                                     :location,
                                                     :location_type,
                                                     :preview_thumbnails,
                                                     :tags,
                                                     :added_date)',
                                                     args_map,
                                                     {location_type: Cassandra::Types.int})

      session.execute_async(statement)
    end

    def self.insert_videos_prepared(session, insert, args)
      video_id = Cassandra::Uuid.new(args['videoid'])
      user_id = Cassandra::Uuid.new(args['userid'])
      location_id = args['location_type'].to_i
      preview_thumbnails = args['preview_thumbnails'].to_h
      tags = Set.new(args['tags'])
      added_date = Time.parse(args['added_date'])

      session.execute_async(insert, arguments: [
                            video_id,
                            user_id,
                            args['name'],
                            args['description'],
                            args['location'],
                            location_id,
                            preview_thumbnails,
                            tags,
                            added_date])
    end
  end
end