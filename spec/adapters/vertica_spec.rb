require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

unless defined?(VERTICA_DB)
  VERTICA_URL = 'vertica://vertica:vertica@localhost:5432/reality_spec' unless defined? VERTICA_URL
  VERTICA_DB = Sequel.connect(ENV['SEQUEL_VERTICA_SPEC_DB']||VERTICA_URL)
end
INTEGRATION_DB = VERTICA_DB unless defined?(INTEGRATION_DB)


VERTICA_DB.create_table! :test do
  varchar :name
  integer :value
end
VERTICA_DB.create_table! :test2 do
  varchar :name
  integer :value
end
VERTICA_DB.create_table! :test3 do
  integer :value
  timestamp :time
end
VERTICA_DB.create_table! :test4 do
  varchar :name, :size => 20
  bytea :value
end

describe "A Vertica database" do 

  before do
    @db = VERTICA_DB
  end

  specify "should correctly parse the schema" do
    @db.schema(:test3, :reload=>true).should == [
      [:value, {:type=>:integer, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"int", :primary_key=>false}],
      [:time, {:type=>:datetime, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"timestamp", :primary_key=>false}]
    ]
    @db.schema(:test4, :reload=>true).should == [
      [:name, {:type=>:string, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"varchar(20)", :primary_key=>false}],
      [:value, {:type=>:blob, :allow_null=>true, :default=>nil, :ruby_default=>nil, :db_type=>"varbinary(80)", :primary_key=>false}]
    ]
  end

end

describe "A vertica dataset" do
  before do
    @d = VERTICA_DB[:test]
    @d.delete if @d.count > 0 # Vertica will throw an error if the table has just been created and does not have a super projection yet.
  end

  specify "should quote columns and tables using double quotes if quoting identifiers" do
    @d.select(:name).sql.should == \
      'SELECT "name" FROM "test"'

    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM "test"'

    @d.select(:max.sql_function(:value)).sql.should == \
      'SELECT max("value") FROM "test"'

    @d.select(:NOW.sql_function).sql.should == \
    'SELECT NOW() FROM "test"'

    @d.select(:max.sql_function(:items__value)).sql.should == \
      'SELECT max("items"."value") FROM "test"'

    @d.order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.select('test.name AS item_name'.lit).sql.should == \
      'SELECT test.name AS item_name FROM "test"'

    @d.select('"name"'.lit).sql.should == \
      'SELECT "name" FROM "test"'

    @d.select('max(test."name") AS "max_name"'.lit).sql.should == \
      'SELECT max(test."name") AS "max_name" FROM "test"'

    @d.insert_sql(:x => :y).should =~ \
      /\AINSERT INTO "test" \("x"\) VALUES \("y"\)( RETURNING NULL)?\z/

  end

  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC'

    @d.reverse_order(:name.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" ASC'

    @d.reverse_order(:name, :test.desc).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'

    @d.reverse_order(:name.desc, :test).sql.should == \
      'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
  end

  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}

    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end

end


describe "A Vertica dataset with a timestamp field" do
  before do
    @db = VERTICA_DB
    @d = @db[:test3]
    @d.delete if @d.count > 0 # Vertica will throw an error if the table has just been created and does not have a super projection yet.
  end
  after do
    @db.convert_infinite_timestamps = false if @db.adapter_scheme == :postgres
  end

  cspecify "should store milliseconds in time fields for Time objects", :do, :swift do
    t = Time.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>1][:time]
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    (t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).should == t.usec
  end

  cspecify "should store milliseconds in time fields for DateTime objects", :do, :swift do
    t = DateTime.now
    @d << {:value=>1, :time=>t}
    t2 = @d[:value =>1][:time]
    @d.literal(t2).should == @d.literal(t)
    t2.strftime('%Y-%m-%d %H:%M:%S').should == t.strftime('%Y-%m-%d %H:%M:%S')
    (t2.is_a?(Time) ? t2.usec : t2.strftime('%N').to_i/1000).should == t.strftime('%N').to_i/1000
  end

end
