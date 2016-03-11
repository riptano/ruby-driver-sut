# Ruby Driver SUT Implementation

As per https://datastax.jira.com/wiki/display/DRIV/L1+-+HTTP+requests+to+CQL+queries

## Installation

### Install JRuby
```bash
# Install dependencies for JRuby
sudo apt-get install git-core curl zlib1g-dev build-essential libssl-dev libreadline-dev libyaml-dev libsqlite3-dev sqlite3 libxml2-dev libxslt1-dev libcurl4-openssl-dev python-software-properties

# Install rbenv
cd
git clone git://github.com/sstephenson/rbenv.git .rbenv
echo 'export PATH="$HOME/.rbenv/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(rbenv init -)"' >> ~/.bashrc
exec $SHELL

git clone git://github.com/sstephenson/ruby-build.git ~/.rbenv/plugins/ruby-build
echo 'export PATH="$HOME/.rbenv/plugins/ruby-build/bin:$PATH"' >> ~/.bashrc
exec $SHELL

# Install JRuby 1.7
rbenv install jruby-1.7.24
rbenv global jruby-1.7.24
rbenv rehash

# Install JRuby 9k
rbenv install jruby-9.0.5.0
rbenv global jruby-9.0.5.0
rbenv rehash
```

### Install Dependencies

```bash
bundle install
```

## Running the server

Make sure the following schema(s) are created in Cassandra:
```
CREATE KEYSPACE killrvideo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE killrvideo;

# User credentials
CREATE TABLE user_credentials (
   email text,
   password text,
   userid uuid,
   PRIMARY KEY (email)
);

# Entity table that will store many videos for a unique user
CREATE TABLE videos (
   videoid uuid,
   userid uuid,
   name varchar,
   description varchar,
   location text,
   location_type int,
   preview_thumbnails map<text,text>,  // <position in video, url of thumbnail>
   tags set<varchar>,
   added_date timestamp,
   PRIMARY KEY (videoid)
);

# Time series wide row with reverse comparator
CREATE TABLE video_event (
   videoid uuid,
   userid uuid,
   event varchar,
   event_timestamp timeuuid,
   video_timestamp bigint,
   PRIMARY KEY ((videoid,userid),event_timestamp,event)
) WITH CLUSTERING ORDER BY (event_timestamp DESC,event ASC);
```

```bash
Usage: server.rb -H [hosts] -V [version] -E [experiment] -S [statement] -G [graphite] -F [frequency]
    -H, --hosts HOSTS                A host to connect to
    -V, --version VERSION            Driver version
    -E, --experiment EXPERIMENT      Experiment name to be run
    -S, --statement STATEMENT        The statement type
    -G, --graphite GRAPHITE          The Graphite server's IP
    -F, --frequency FREQUENCY        Frequency of reporting metrics to Graphite
    -h, --help                       Show this message

# Example
ruby -I ../ruby-driver/lib/ server.rb -H 127.0.0.1 -V 3.0.0 -E video_event -S prepared -G 104.197.106.246 -F 10
```
