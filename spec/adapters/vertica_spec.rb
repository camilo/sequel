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
