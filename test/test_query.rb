require File.expand_path '../helper.rb', __FILE__
require File.expand_path '../models/person.rb', __FILE__
require File.expand_path '../models/post.rb', __FILE__

RESET_SQL = File.read(File.expand_path('../db/reset.sql', __FILE__))

describe BulkUpsert::Query do

  before do
    ActiveRecord::Base.connection.execute(RESET_SQL)
  end

  describe "attribute values are not normalized" do
    it "converts values for search" do
      existing = Person.create name: "John Doe"
      updated  = BulkUpsert.build Person,
        { name: "    John Doe     " },
        { age: 32 }
  
      BulkUpsert.save_group([updated])
      refute_nil updated.id
      assert_equal 1, Person.count
      assert_equal existing.id, updated.id
      assert_equal 32, existing.reload.age
    end

    it "converts values for update" do
      existing = Person.create age: 30
      updated  = BulkUpsert.build Person,
        { age: 30 },
        { name: "    John Doe     " }
  
      BulkUpsert.save_group([updated])
      refute_nil updated.id
      assert_equal 1, Person.count
      assert_equal existing.id, updated.id
      assert_equal "John Doe", existing.reload.name
    end
  end

  describe "list contains duplicates" do
    it "inserts only one record by default" do
      first  = BulkUpsert.build Person, name: "John Doe"
      second = BulkUpsert.build Person, name: "John Doe"
  
      BulkUpsert.save_group([first, second])
      assert_equal 1, Person.count
      refute_nil first.id
      refute_nil second.id
    end

    it "inserts all duplicates when `skip_find` flag is set" do
      first  = BulkUpsert.build Person, name: "John Doe"
      second = BulkUpsert.build Person, name: "John Doe"
  
      BulkUpsert.save_group([first, second], skip_find: true)
      assert_equal 2, Person.count
      refute_equal first.id, second.id
      refute_nil first.id
      refute_nil second.id
    end
  end

  describe "attribute is a `belongs_to` association" do
    it "resolves attribute name to foreign key" do
      person = BulkUpsert.build Person, name: "John Doe"
      post = BulkUpsert.build Post, person: person, topic: "Test"
  
      BulkUpsert.save_group([person])
      assert_equal 1, Person.count
      refute_nil person.id

      BulkUpsert.save_group([post], allow_belongs_to: true)
      assert_equal 1, Post.count
      assert_equal person.id, post.model.person_id
      refute_nil post.id
    end
  end

  describe "list contains invalid models" do
    it "skips invalid models" do
      valid   = BulkUpsert.build Post, topic: "Test"
      invalid = BulkUpsert.build Post, topic: nil
      BulkUpsert.save_group([valid, invalid])
  
      assert_equal 1, Post.count
      assert_nil invalid.id
      refute_nil valid.id
    end
  end

  describe "model has `maybe` flags" do
    it "skips update for defined existing attribute" do
      existing = Person.create name: "John Doe", age: 30
      updated  = BulkUpsert.build Person, name: "John Doe"
      updated.maybe :age, 32
  
      BulkUpsert.save_group([updated])
      assert_equal 1, Person.count
      assert_equal 30, existing.reload.age
    end
  
    it "updates model with undefined existing attribute" do
      existing = Person.create name: "John Doe"
      updated  = BulkUpsert.build Person, name: "John Doe"
      updated.maybe :age, 32
  
      BulkUpsert.save_group([updated])
      assert_equal 1, Person.count
      assert_equal 32, existing.reload.age
    end  
  end

  describe "model has `prefer` flags" do
    it "skips update for undefined new attribute" do
      existing = Person.create name: "John Doe", age: 30
      updated  = BulkUpsert.build Person, name: "John Doe"
      updated.prefer :age, nil
  
      BulkUpsert.save_group([updated])
      assert_equal 1, Person.count
      assert_equal 30, existing.reload.age
    end
  
    it "updates model with defined new attribute" do
      existing = Person.create name: "John Doe", age: 30
      updated  = BulkUpsert.build Person, name: "John Doe"
      updated.prefer :age, 32
  
      BulkUpsert.save_group([updated])
      assert_equal 1, Person.count
      assert_equal 32, existing.reload.age
    end
  end

  describe "model has `always` flags" do
    it "updates model with undefined existing attribute" do
      existing = Person.create name: "John Doe"
      updated  = BulkUpsert.build Person, name: "John Doe"
      updated.always :age, 32
  
      BulkUpsert.save_group([updated])
      assert_equal 1, Person.count
      assert_equal 32, existing.reload.age
    end
  
    it "updates model with undefined new attribute" do
      existing = Person.create name: "John Doe", age: 30
      updated  = BulkUpsert.build Person, name: "John Doe"
      updated.always :age, nil
  
      BulkUpsert.save_group([updated])
      assert_equal 1, Person.count
      assert_nil existing.reload.age
    end
  end

  describe "search atts include NULL values" do
    it "throws without `allow_nulls` flag" do
      updated = BulkUpsert.build Person,
        { name: "John Doe", age: nil },
        { age: 30 }

      assert_raises BulkUpsert::MissingValueError do
        BulkUpsert.save_group([updated])
      end
    end

    it "updates model when `allow_nulls` flag is set" do
      existing = Person.create name: "John Doe"
      updated  = BulkUpsert.build Person,
        { name: "John Doe", age: nil },
        { age: 30 }

      BulkUpsert.save_group([updated], allow_nulls: true)
      assert_equal 1, Person.count
      assert_equal existing.id, updated.id
      assert_equal 30, existing.reload.age
    end
  end

  describe "search atts include JSON values" do
    it "creates new record" do
      updated = BulkUpsert.build Person,
        extra: { maiden_name: "Smith" }

      BulkUpsert.save_group([updated])
      refute_nil updated.id
      assert_equal 1, Person.count
      assert_equal "Smith", Person.first.extra["maiden_name"]
    end

  end

  describe "same attribute has multiple flags" do
    it "throws if specified within one model" do
      record = BulkUpsert.build Person, name: "John Doe"
      record.prefer :age, 40
      record.maybe :age, 20

      assert_raises BulkUpsert::InconsitentFlagError do
        BulkUpsert.save_group([record])
      end
    end

    it "throws if specified within a group of models" do
      first = BulkUpsert.build Person, name: "John Doe"
      second = BulkUpsert.build Person, name: "Jane Doe"
      first.prefer :age, 40
      second.maybe :age, 20

      assert_raises BulkUpsert::InconsitentFlagError do
        BulkUpsert.save_group([first, second])
      end
    end
  end

  describe "no attributes have been specified" do
    it "throws during save" do
      record = BulkUpsert.build Person, {}
      assert_raises BulkUpsert::EmptySearchListError do
        BulkUpsert.save_group([record])
      end
    end
  end

  describe "ID column in included as an attribute to search" do
    it "throws when saving" do
      person = BulkUpsert.build Person, { id: 65465465 }
      BulkUpsert.save_group([person])
      assert_equal 65465465, person.id
      assert_equal 1, Person.count
    end
  end

  describe "ID column in included as an attribute to update" do
    it "throws when saving" do
      record = BulkUpsert.build Person,
        { name: "John Doe" },
        { id: 10 }

      assert_raises BulkUpsert::PrimaryKeyUpdateError do
        BulkUpsert.save_group([record])
      end
    end
  end

  describe "different sets of attributes are used for search" do
    it "throws when saving" do
      first  = BulkUpsert.build Person, name: "John Doe"
      second = BulkUpsert.build Person, bio: 30

      assert_raises BulkUpsert::InconsitentAttributeError do
        BulkUpsert.save_group([first, second])
      end
    end
  end

  describe "different models are included within one group" do
    it "throws when saving" do
      person = BulkUpsert.build Person, name: "John Doe"
      post   = BulkUpsert.build Post, topic: "Test"

      assert_raises BulkUpsert::MultipleClassesError do
        BulkUpsert.save_group([person, post])
      end
    end
  end

  describe "attribute is a belongs_to association without optional flag" do
    it "throws without `allow_belongs_to` flag" do
      person = Person.create name: "John Doe"
      post   = BulkUpsert.build Post, person_id: person.id, topic: "Test"

      assert_raises BulkUpsert::BelongsToDeficiencyError do
        BulkUpsert.save_group([post])
      end
    end

    it "updates model when `allow_belongs_to` flag is sets" do
      person = Person.create name: "John Doe"
      post   = BulkUpsert.build Post, person_id: person.id, topic: "Test"

      BulkUpsert.save_group([post], allow_belongs_to: true)
      assert_equal 1, Post.count
      refute_nil post.id
    end
  end

  describe "with `skip_find` flag set" do
    it "forces inserts" do
      existing  = Person.create name: "John Doe"
      duplicate = BulkUpsert.build Person, { name: "John Doe" }

      BulkUpsert.save_group([duplicate], skip_find: true)
      refute_nil duplicate.id
      refute_equal duplicate.id, existing.id
      assert_equal 2, Person.count
    end
  end

  describe "with `skip_id_assignment` flag set" do
    it "does not resolve IDs" do
      record = BulkUpsert.build Person, { name: "John Doe" }

      BulkUpsert.save_group([record], skip_id_assignment: true)
      assert_nil record.id
      assert_equal 1, Person.count
    end
  end

end
