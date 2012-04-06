require 'vertica'

module Sequel
  module Vertica
    class Database < Sequel::Database
      set_adapter_scheme :vertica

      def initialize(opts={})
        super
      end

      def connect(server)
        opts = server_opts(server)
        ::Vertica::Connection.new(
          :host => opts[:host],
          :user => opts[:user],
          :password => opts[:password],
          :port => opts[:port],
          :schema => opts[:schema],
          :database => opts[:database],
          :ssl => opts[:ssl] )
      end

      def execute(sql, opts={}, &block)
        synchronize(opts[:server]) do |conn|
          conn.query(sql, &block)
        end
      end

      def supports_create_table_if_not_exists?
        true
      end

      def supports_drop_table_if_exists?
        true
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def fetch_rows(sql)
      end
    end
  end
end
