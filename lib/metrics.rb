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
  class Metrics
    class Statistic
      attr_accessor :throughput, :latency, :num_errors, :errors

      def initialize
        @throughput = Hash.new(0)
        @latency = Hash.new { |hash, key| hash[key] = [] }
        @num_errors = 0
        @errors = Hash.new { |hash, key| hash[key] = [] }
      end
    end

    attr_accessor :statistics

    def initialize(experiment, statement_type)
      @statistics = Hash.new
      @statistics["#{statement_type}.insert.#{experiment}"] = Statistic.new
      @statistics["#{statement_type}.select.#{experiment}"] = Statistic.new
      @lock = ::Monitor.new
    end

    def record_metric(statement, future, start_time)
      future.on_complete do |value, error|
        curr_time = Time.new
        if value
          latency = (curr_time - start_time) * 1000
          @lock.synchronize do
            @statistics[statement].latency[curr_time.to_i] << latency
            @statistics[statement].throughput[curr_time.to_i] += 1
          end

          value
        else
          @lock.synchronize do
            @statistics[statement].num_errors += 1
            @statistics[statement].errors[curr_time.to_i] << error
          end

          error
        end
      end

      future
    end

    def reset(name)
      @statistics[name] = Statistic.new
    end

  end
end
