class Post < ActiveRecord::Base
  validates :topic, presence: true
  belongs_to :person
end
