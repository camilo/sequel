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

      def supports_transaction_isolation_levels?
        true
      end

      def identifier_input_method_default
        nil
      end

      def identifier_output_method_default
        nil
      end

      def schema_parse_table(table_name, opts)
        metadata_dataset.select(:column_name, 
          :is_nullable.as(:allow_null),
         (:column_default).as(:default),
         (:data_type).as(:db_type)
        ).filter(:table_name => table_name).from(:columns).map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          row[:primary_key] = false
          [row.delete(:column_name).to_sym, row]
        end
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def fetch_rows(sql)
        execute(sql) { |row| yield row }
      end
    end
  end
end
