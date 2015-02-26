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
      when /\/simple\-statements\/users\/(\d+)(\/\d+)?/
        range_start = Integer($1)
        range_end   = range_start + ($2 ? Integer($2[1..-1]) : 1)

        case env['REQUEST_METHOD']
        when 'POST'
          futures = (range_start...range_end).map do |id|
            username = "user-#{id}"
            session.execute_async(
              "INSERT INTO videodb.users (username, firstname, lastname, password, email, created_date) VALUES (?, ?, ?, ?, ?, ?)", arguments: [
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
              "SELECT username, firstname, lastname, password, email, created_date FROM videodb.users WHERE username = ?", arguments: [username]
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
      when /\/prepared\-statements\/users\/(\d+)(\/\d+)?/
        range_start = Integer($1)
        range_end   = range_start + ($2 ? Integer($2[1..-1]) : 1)

        case env['REQUEST_METHOD']
        when 'POST'
          statement = session.prepare("INSERT INTO videodb.users (username, firstname, lastname, password, email, created_date) VALUES (?, ?, ?, ?, ?, ?)")
          futures   = (range_start...range_end).map do |id|
            username = "user-#{id}"
            session.execute_async(
              statement, arguments: [
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
          statement = session.prepare("SELECT username, firstname, lastname, password, email, created_date FROM videodb.users WHERE username = ?")
          futures   = (range_start...range_end).map do |id|
            username = "user-#{id}"
            session.execute_async(
              statement, arguments: [username]
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
