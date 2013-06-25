=begin

PtStream - PowerTrack streaming class.

Written to manage the streaming of a single PowerTrack connection. If more than one stream is needed, multiple instances
of this class can be spun up.

This is being written with an eye on using it as a Rails background process, streaming activities and writing them to a
local database.  The Rails application will ride on top of this database.

=end

require_relative "./http_stream"  #based on gnip-stream project at (https://github.com/rweald/gnip-stream).
require 'base64'
require 'active_record'
require 'mysql2'
require 'time'
require 'optparse'

class Activity < ActiveRecord::Base
    validates :native_id, uniqueness: {scope: :publisher}
end

class PtStream

    attr_accessor :stream, :account_name, :user_name, :password_encoded,
                  :publisher, :stream_type, :stream_label,
                  :url,
                  :database_adapter, :db_host, :db_schema, :db_user_name, :db_password,
                  :activities

    def initialize(config)

        @activities = Array.new  #TODO: may want to use Queue class instead (thread safe).

        if not config.nil? then
            getConfig(config)
        end
    end

    def getPassword
        #You may want to implement a more secure password handler.  Or not.
        @password = Base64.decode64(@password_encoded)  #Decrypt password.
    end

    def getConfig(config_file)

        config = YAML.load_file(config_file)

        #Set account values.
        @account_name = config['account']['name']
        @user_name = config['account']['user_name']
        @password_encoded = config['account']['password_encoded']
        @password = getPassword

        #Set stream details.
        @publisher = config['stream']['publisher']
        @stream_type = config['stream']['type']
        @stream_label = config['stream']['label']

        @url = setURL

        #Set database details.
        @database_adapter = config['database']['adapter']
        @db_host = config['database']['host']
        @db_schema = config['database']['schema']
        @db_user_name = config['database']['user_name']
        @db_password = config['database']['password']

        ActiveRecord::Base.establish_connection(
            :adapter => @database_adapter,
            :host => @db_host,
            :username => @db_user_name,
            :password => @db_password,
            :database => @db_schema
        )
    end

    def setURL
        "https://stream.gnip.com:443/accounts/#{@account_name}/publishers/#{@publisher}/streams/#{@stream_type}/#{@stream_label}.json"
    end

    #NativeID is defined as a string.  This works for Twitter, but not for other publishers who use alphanumerics.
    #Tweet "id" field has this form: "tag:search.twitter.com,2005:198308769506136064"
    #This function parses out the numeric ID at end.
    def getNativeID(data)

        id= data["id"]

        if @publisher == "twitter" then
            native_id = id.split(":")[-1]
        end

        if @publisher == "tumblr" then
            native_id = id.split("/")[-2]
        end

        #These comment/like/vote streams have a blog ID, post ID and a comment ID, so I decided to capture all three.
        if @publisher.include?("wordpress") then
            if @stream_type == "post" then
                native_id = id.split("/")[-3] + "-" + id.split("/")[-1]
            else
                native_id = id.split("/")[-5] + "-" + id.split("/")[-3] + "-" + id.split("/")[-1]
            end
        end

        #These comment/vote streams have a thread ID and a comment/vote ID, so I decided to capture both.
        if @publisher == "automattic" or @publisher == "intensedebate" then

            if @stream_type == "vote" then
                native_id = id.split("/")[-5] + "-" + id.split("/")[-3]
            else
                native_id = id.split("/")[-3] + "-" + id.split("/")[-1]
            end
        end

        if @publisher == "foursquare" or @publisher == "newsgator" or @publisher == "stocktwits" then
            native_id = id.split("/")[-1]
        end

        if @publisher == "getglue" then
            if data["verb"] != "user_protect" then
                native_id = id.split("/")[-2] + "-" + id.split("/")[-1]
            else
                native_id = id.split("/")[-1]
            end
        end

        return native_id
    end

    #Twitter uses UTC.
    def getPostedTime(data)


        if @publisher == "stocktwits" then
            time_stamp = Time.parse(data["object"]["postedTime"]).strftime("%Y-%m-%d %H:%M:%S")
        else

            time_stamp = data["postedTime"]

            if not time_stamp.nil? then
                time_stamp = Time.parse(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
            else #This an activity with our a PostedTime, such as a Tumblr post delete...
                p "Using Now for timestamp..."
                time_stamp = Time.now.strftime("%Y-%m-%d %H:%M:%S")
            end
        end

        time_stamp
    end

    def getGeoCoordinates(activity)

        #safe defaults... well, sort of...  defaulting to off the west coast of Africa...
        latitude = 0
        longitude = 0

        if @publisher == "twitter" then

            geo = activity["geo"]

            if not geo.nil? then #We have a "root" geo entry, so go there to get Point location.
                if geo["type"] == "Point" then
                    latitude = geo["coordinates"][0]
                    longitude = geo["coordinates"][1]

                    #We are done here, so return
                    return latitude, longitude
                end
            end
        end

        if @publisher == "foursquare" then
            geo = activity["object"]["geo"]["coordinates"]
            longitude = geo[0]
            latitude = geo[1]
        end

        return latitude, longitude
    end


    #Returns a comma-delimited list of rule values and tags.
    #values, tags
    def getMatchingRules(matching_rules)
        values = ""
        tags = ""
        matching_rules.each do |rule|
            values = values + rule["value"] + ","
            if not rule["tag"].nil?
                tags = tags + rule["tag"] + ","
            else
                tags = ""
            end
        end

        return values.chomp(","), tags.chomp(",")
    end


    def getPlace(data)

        place = data["location"]

        if not place.nil? then
            place = data["location"]["displayName"]
        end

        place

    end

    #Parse the body/message/post of the activity.
    def getBody(data)

        if @publisher == "newsgator" or (@publisher.include?("wordpress") and @stream_type == "post") then
            body = data["object"]["content"]
        elsif @publisher == "getglue" then
             verb = data["verb"]
             if verb == "share" then
                 body = data["body"]
             elsif verb == "post" or verb == "like" or verb == "follow" or verb == "receive" or verb == "checkin" or verb == "reject" or "user_protect" then
                 body = data["displayName"]
             elsif verb == "vote" then
                 body = data["displayName"]
             end
        else
            body = data["body"]
        end

        body
    end

    '''
    Parses normalized Activity Stream JSON.
    Parsing details here are driven by the current database schema used to store activities.
    If writing files, then just write out the entire activity payload to the file.
    '''
    def processResponseJSON(activity)

        p activity

        if activity.include?("delete") then   #TODO: Need to test on VERB, not content.
            activity.gsub!('\"','"')   #Testing and tripping over Tumblr delete activities prompted the special
            #handling of the activity JSON format.
        end

        begin
            data = JSON.parse(activity)
        rescue => e
            p e.message
            p e.backtrace
            return #Could not parse the activity, move on to next... Would want to log the offending activity...
        end

        #It is wise to store the entire activity payload, in case you need to parse it later...
        content = activity

        #Parse from the activity the "atomic" elements we are inserting into db fields.
        posted_at = getPostedTime(data)
        native_id = getNativeID(data)
        body = getBody(data)

        #Only PowerTrack ("track") streams have the rules (and tags) to parse...
        if @stream_type == "track" then
            #Parse gnip:matching_rules and extract one or more rule values/tags
            rule_values, rule_tags = getMatchingRules(data["gnip"]["matching_rules"])
        else
            rule_values = "firehose"
        end

        #Parse the activity and extract any geo available data.
        latitude, longitude = getGeoCoordinates(data)

        #These are not parsed/handled yet:
        #place = getPlace(data)
        #if not data["actor"]["location"]["displayName"].nil?
        #    bio_place = data["actor"]["location"]["displayName"]
        #end
        place = ""
        bio_place = ""
        stream_id = 1

        begin

            p native_id + " --> " + body

            Activity.create(:native_id => native_id,
                            :publisher => @publisher,
                            :content => content,
                            :body => body,
                            :rule_value => rule_values,
                            :rule_tag => rule_tags,
                            :posted_at => posted_at,
                            :longitude => longitude,
                            :latitude => latitude,
                            :place => place,
                            :bio_place => bio_place,
                            :stream_id => stream_id
            )
        rescue => e
            p e.message
            p e.backtrace
        end

        p "Activity added to database..."

    end

    #There is one thread for streaming/consuming data, and it calls this.
    def consumeStream(stream)
        begin
            stream.consume do |message|
                @activities << message #Add to end of array.
                if @activities.length > 1000 then
                    "Queueing #{@activities.length} activities..."
                end
                #puts "#{message}"
            end
        rescue => e
            p "Error occurred: #{e.message}"
            consumeStream(@stream)
        end
    end

    #There is one thread for storing @activities, and it is calls this.
    def storeActivities
        while true
            while @activities.length > 0
                activity = @activities.shift  #FIFO, popping from start of array.
                processResponseJSON(activity)
            end
            sleep (1)
        end
    end


    #Single thread for streaming, and another for handling received data.
    def streamData

        threads = []  #Thread pool.

        @stream = PowertrackStream.new(@url,@user_name, @password)

        t = Thread.new {Thread.pass; consumeStream(stream)}

        begin
            t.run
        rescue ThreadError => e
            p "Threading error: #{e.message}"
        rescue => e
            p "Error: #{e.message}"
        end

        threads << t  #Add it to our pool (array) of threads.

        #OK, add a thread for consuming from @activities.
        #This thread sends activities to the database.
        t = Thread.new {storeActivities}

        begin
            t.run
        rescue ThreadError => e
            p e.message
        end

        threads << t #Add it to our pool (array) of threads.

        threads.each do |t|
            begin
                t.join
            rescue ThreadError => e
                p "Threading error: #{e.message}"
            rescue => e
                p "Error: #{e.message}"
            end
        end

    end

end


#=======================================================================================================================
if __FILE__ == $0  #This script code is executed when running this file.

    OptionParser.new do |o|
        o.on('-c CONFIG') { |config| $config = config}
        o.parse!
    end

    if $config.nil? then
        $config = "./config_private.yaml"  #Default
    end

    p "Creating Streaming Client object with config file: " + $config

    pt = PtStream.new($config)
    pt.streamData

end