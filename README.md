Introduction
============

This is a Ruby script written to stream data from a PowerTrack stream.  Retrieved data are written to standard out or a local MySQL database.  Database writes are based on ActiveRecord (see below for database schema details).

HTTP streaming is based on the gnip-stream project at https://github.com/rweald/gnip-stream.

The target PowerTrack stream is configured in a configuration file. If you want to use this script to stream from more than one stream at a time, in theory, multiple instances of this code could be ran (with different configuration files passed in).  

Once the script is started, it creates a streaming connection to the configured data feed. 


Note: Need to use em-http-request version 1.0.3.  Current version serves up strangely concated activities where two activities are randomly conjoined
 

Usage
=====

One file is passed in at the command-line if you are running this code as a script (and not building some wrapper
around the PtStream class):

1) A configuration file with account/username/password, stream and database details (see below, or the sample project
file, for details):  -c "./config.yaml"

The configuration file needs to have an "account" section, an "stream" section, and a "database" section.  

So, if you were running from a directory with this source file in it, with the configuration file in that folder too,
the command-line would look like this:

        $ruby ./pt_stream.rb -c "./config.yaml"



Configuration
=============

See the sample config.yaml file for an example of a PowerTrack client configuration file.  

Here are some important points:

<p>
+ In the "account" section, you specify your account "name".  Along with the "publisher", "type", and "label" settings in the "stream" section, the PowerTrack end-point URL will be constructed.

	https://stream.gnip.com:443/accounts/<name>/publishers/<publisher>/streams/<type>/<label>.json

The "account" section also contains the PowerTrack credentials used to authenticate with the PowerTrack system.

</p>


Database details
================

The script writes to a local MySQL database.  The database schema assumed by the script is encapsulated in the ActiveRecord *create_table* statement below.  These details should be updated to match the schema you use to store activities.

```
create_table "activities", :force => true do |t|
    t.string   "native_id"
    t.string   "publisher"
    t.string   "content"
    t.string   "body"
    t.string   "rule_value"
    t.string   "rule_tag"
    t.datetime "posted_at"
    t.datetime "created_at", :null => false
    t.datetime "updated_at", :null => false
    t.float    "latitude"
    t.float    "longitude"
    t.string   "place"
    t.string   "bio_place"
    t.integer  "stream_id"
  end
```



