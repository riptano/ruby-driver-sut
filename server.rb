require 'bundler/setup'
require 'cassandra'
require 'torquebox/web'

def app(cluster)
  session = cluster.connect

  Proc.new do |env|
    begin
      case env['REQUEST_URI']
      when '/'
        if env['REQUEST_METHOD'] == 'GET'
          ['200', {'Content-Type' => 'text/plain'}, ['Hello World']]
        else
          ['404', {'Content-Type' => 'text/plain'}, ['Not Found']]
        end
      when '/cassandra'
        if env['REQUEST_METHOD'] == 'GET'
          session.execute('SELECT NOW() from system.local')
          ['204', {}, []]
        else
          ['404', {'Content-Type' => 'text/plain'}, ['Not Found']]
        end
      when '/keyspace'
        case env['REQUEST_METHOD']
        when 'DELETE'
          begin
            session.execute('DROP KEYSPACE videodb')
            ['204', {}, []]
          rescue Cassandra::Errors::ConfigurationError
            ['304', {'Content-Type' => 'text/plain'}, []]
          end
        when 'POST'
          begin
            session.execute('CREATE KEYSPACE videodb WITH REPLICATION = { \'class\' : \'SimpleStrategy\', \'replication_factor\' : 3 }')
            session.execute('USE videodb')
            session.execute('CREATE TABLE users ( username varchar, firstname varchar, lastname varchar, email list<varchar>, password varchar, created_date timestamp, PRIMARY KEY (username) )')
            session.execute('CREATE TABLE videos ( videoid uuid, videoname varchar, username varchar, description varchar, location map<varchar,varchar>, tags set<varchar>, upload_date timestamp, PRIMARY KEY (videoid) )')
            session.execute('CREATE TABLE username_video_index ( username varchar, videoid uuid, upload_date timestamp, videoname varchar, PRIMARY KEY (username,upload_date,videoid) ) WITH CLUSTERING ORDER BY (upload_date DESC)')
            session.execute('CREATE TABLE video_rating ( videoid uuid, rating_counter counter, rating_total counter, PRIMARY KEY (videoid) )')
            session.execute('CREATE TABLE tag_index ( tag varchar, videoid uuid, tag_ts timestamp, PRIMARY KEY (tag, videoid) )')
            session.execute('CREATE TABLE comments_by_video ( videoid uuid, username varchar, comment_ts timeuuid, comment varchar, PRIMARY KEY (videoid,comment_ts,username) ) WITH CLUSTERING ORDER BY (comment_ts DESC, username ASC)')
            session.execute('CREATE TABLE comments_by_user ( username varchar, videoid uuid, comment_ts timeuuid, comment varchar, PRIMARY KEY (username,comment_ts,videoid) ) WITH CLUSTERING ORDER BY (comment_ts DESC, videoid ASC)')
            session.execute('CREATE TABLE video_event ( videoid uuid, username varchar, event varchar, event_timestamp timeuuid, video_timestamp bigint, PRIMARY KEY ((videoid,username),event_timestamp,event) ) WITH CLUSTERING ORDER BY (event_timestamp DESC,event ASC)')
            ['201', {'Content-Type' => 'text/plain'}, ['Created']]
          rescue Cassandra::Errors::AlreadyExistsError
            ['304', {'Content-Type' => 'text/plain'}, []]
          end
        when 'PUT'
          begin
            session.execute('USE videodb')
            ['204', {}, []]
          rescue Cassandra::Errors::InvalidError
            ['304', {'Content-Type' => 'text/plain'}, []]
          end
        else
          ['404', {'Content-Type' => 'text/plain'}, ['Not Found']]
        end
      when /\/simple\-statements\/users\/(\d+)(\/\d+)?/
        range_start = Integer($1)
        range_end   = range_start + ($2 ? Integer($2[1..-1]) : 1)

        case env['REQUEST_METHOD']
        when 'POST'
          futures = (range_start...range_end).map do |id|
            username = "user-#{id}"
            session.execute_async(
              "INSERT INTO users (username, firstname, lastname, password, email, created_date) VALUES (?, ?, ?, ?, ?, ?)", arguments: [
              username, "First Name #{id}", "Last Name #{id}", 'password',
              ["#{username}@relational.com", "#{username}@nosql.com"], Time.now
            ])
          end
          begin
            Cassandra::Future.all(futures).get
            ['200', {'Content-Type' => 'text/plain'}, ['OK']]
          rescue Cassandra::Errors::InvalidError => e
            ['409', {'Content-Type' => 'text/plain'}, [e.message]]
          end
        when 'GET'
          futures = (range_start...range_end).map do |id|
            username = "user-#{id}"
            session.execute_async(
              "SELECT username, firstname, lastname, password, email, created_date FROM users WHERE username = ?", arguments: [username]
            ).then do |rows|
              row = rows.first
              row && row['username']
            end
          end
          begin
            usernames = Cassandra::Future.all(futures).get
            ['200', {'Content-Type' => 'text/plain'}, [usernames.join(',')]]
          rescue Cassandra::Errors::InvalidError => e
            ['409', {'Content-Type' => 'text/plain'}, [e.message]]
          end
        else
          ['404', {'Content-Type' => 'text/plain'}, ['Not Found']]
        end
      else
        ['404', {'Content-Type' => 'text/plain'}, ['Not Found']]
      end
    rescue => e
      ['500', {'Content-Type' => 'text/plain'}, ["#{e.class.name}: #{e.message}"]]
    end
  end
end

TorqueBox::Web.run(
  rack_app: app(Cassandra.cluster),
  host: '0.0.0.0',
  port: 6000
).run_from_cli
