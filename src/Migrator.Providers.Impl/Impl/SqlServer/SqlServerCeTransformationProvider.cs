#region License

//The contents of this file are subject to the Mozilla Public License
//Version 1.1 (the "License"); you may not use this file except in
//compliance with the License. You may obtain a copy of the License at
//http://www.mozilla.org/MPL/
//Software distributed under the License is distributed on an "AS IS"
//basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
//License for the specific language governing rights and limitations
//under the License.

#endregion

using System;
using System.Data;
using System.Data.SqlServerCe;
using Migrator.Framework;
using System.Collections.Generic;

namespace Migrator.Providers.SqlServer
{
	/// <summary>
	/// Migration transformations provider for Microsoft SQL Server Compact Edition.
	/// </summary>
	public class SqlServerCeTransformationProvider : SqlServerTransformationProvider
	{
		public SqlServerCeTransformationProvider(Dialect dialect, string connectionString)
			: base(dialect, connectionString)
		{

		}

		protected override void CreateConnection()
		{
			_connection = new SqlCeConnection();
			_connection.ConnectionString = _connectionString;
			_connection.Open();
		}

		public override bool ConstraintExists(string table, string name)
		{
			using (IDataReader reader =
                ExecuteQuery(string.Format("SELECT cont.constraint_name FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS cont WHERE cont.Constraint_Name='{0}' AND TABLE_NAME='{1}'", name, table)))
			{
				return reader.Read();
			}
		}

        public override bool IndexExists(string table, string name)
        {
            using (IDataReader reader =
                ExecuteQuery(string.Format("SELECT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_Name='{0}' AND TABLE_NAME='{1}'", name, table)))
            {
                return reader.Read();
            }
        }

        public override void RemoveIndex(string table, string name)
        {
            if (TableExists(table) && IndexExists(table, name))
            {
                ExecuteNonQuery(String.Format("DROP INDEX {0}.{1}", table, name));
            }
        }

        public override bool TableExists(string table) {
            string tableWithoutBrackets = this.RemoveBrackets(table);
            string tableName = this.GetTableName(tableWithoutBrackets);
            using (IDataReader reader = 
                ExecuteQuery(String.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{0}'", tableName))) {
                return reader.Read();
            }
        }

        protected new string GetSchemaName(string longTableName)
        {
            throw new MigrationException("SQL CE does not support database schemas.");
        }

        // This needs a little work. Only supports column properties NULL / NOT NULL
        public override Column[] GetColumns(string table)
        {
			List<Column> columns = new List<Column>();
			using (
				IDataReader reader =
					ExecuteQuery(
						String.Format("select COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_HASDEFAULT, COLUMN_DEFAULT from information_schema.columns where table_name = '{0}'", table)))
			{
				while (reader.Read())
				{
					Column column = new Column(reader.GetString(0));
                    bool isNullable = reader.GetString(1) == "YES";
                    var dbType = Dialect.GetDbType(reader.GetString(2));
                    bool hasDefault = reader.GetBoolean(3);
                    var columnDefault = reader.GetValue(4);

					column.ColumnProperty |= isNullable ? ColumnProperty.Null : ColumnProperty.NotNull;
                    column.Type = dbType;
                    if (hasDefault)
                        column.DefaultValue = columnDefault;

					columns.Add(column);
				}
			}

			return columns.ToArray();
        }

		public override void RenameColumn(string tableName, string oldColumnName, string newColumnName)
		{
			if (ColumnExists(tableName, newColumnName))
				throw new MigrationException(String.Format("Table '{0}' has column named '{1}' already", tableName, newColumnName));

			if (ColumnExists(tableName, oldColumnName))
			{
				Column column = GetColumnByName(tableName, oldColumnName);

                ColumnProperty? oldColumnProperty = null;
                Column newColumn;
                if (column.ColumnProperty.HasFlag(ColumnProperty.Null) || column.DefaultValue != null)
                {
                    // Column accepts nulls or has a default value. Go ahead and just create the new column matching the old
                    newColumn = new Column(newColumnName, column.Type, column.ColumnProperty, column.DefaultValue);
                }
                else
                {
                    // Column doesn't accept nulls and no default value has been set. Create the new column so it allows nulls
                    oldColumnProperty = column.ColumnProperty;
                    var tmpColumnProperty = column.ColumnProperty;
                    tmpColumnProperty |= ColumnProperty.Null; // Set flag
                    tmpColumnProperty &= ~ColumnProperty.NotNull; // Unset flag

                    newColumn = new Column(newColumnName, column.Type, tmpColumnProperty);
                }

				AddColumn(tableName, newColumn);
				ExecuteNonQuery(string.Format("UPDATE {0} SET {1}={2}", tableName, newColumnName, oldColumnName));
				RemoveColumn(tableName, oldColumnName);

                if (oldColumnProperty.HasValue)
                {
                    // We modified the column to allow nulls before, so we update the column to match the old column now
                    newColumn.ColumnProperty = oldColumnProperty.Value;
                    ChangeColumn(tableName, newColumn);
                }
			}
		}

		// Not supported by SQLCe when we have a better schemadumper which gives the exact sql construction including constraints we may use it to insert into a new table and then drop the old table...but this solution is dangerous for big tables.
		public override void RenameTable(string oldName, string newName)
		{
			
			if (TableExists(newName))
				throw new MigrationException(String.Format("Table with name '{0}' already exists", newName));

			//if (TableExists(oldName))
			//    ExecuteNonQuery(String.Format("EXEC sp_rename {0}, {1}", oldName, newName));
		}

		protected override string FindConstraints(string table, string column)
		{
			return
				string.Format("SELECT cont.constraint_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cont "
					+ "WHERE cont.Table_Name='{0}' AND cont.column_name = '{1}'",
					table, column);
		}
	}
}
