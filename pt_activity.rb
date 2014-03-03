require 'active_record'

class Activity < ActiveRecord::Base
    validates :native_id, uniqueness: {scope: :publisher}
end
