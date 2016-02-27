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
  class Util
    def self.ok_200(message = 'OK')
      ['200', {'Content-Type' => 'text/plain'}, [message]]
    end
    def self.no_content_204
      ['204', {}, []]
    end
    def self.not_found_404
      ['404', {'Content-Type' => 'text/plain'}, ['Not Found']]
    end
    def self.conflict_409(message)
      ['409', {'Content-Type' => 'text/plain'}, [message]]
    end
    def self.server_error_500(message)
      ['500', {'Content-Type' => 'text/plain'}, [message]]
    end
  end
end