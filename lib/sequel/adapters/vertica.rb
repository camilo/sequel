require 'vertica'

module Sequel
  module Vertica
    class Database < Sequel::Database
      ::Vertica::Connection.send(:alias_method,:execute, :query)
      PK_NAME = 'C_PRIMARY'
      set_adapter_scheme :vertica

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
          res = conn.query(sql)
          res.each(&block)
        end
      end

      alias_method :execute_insert, :execute
      alias_method :execute_dui, :execute

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
        selector = [:column_name, :constraint_name, :is_nullable.as(:allow_null), 
                    (:column_default).as(:default), (:data_type).as(:db_type)]

        dataset = metadata_dataset.select(*selector).filter(:table_name => table_name).
          from(:columns).left_outer_join(:table_constraints, :table_id => :table_id)
        
        dataset.map do |row|
          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          row[:primary_key] = row.delete(:constraint_name) == PK_NAME
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
