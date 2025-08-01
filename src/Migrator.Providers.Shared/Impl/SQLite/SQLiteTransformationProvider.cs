using System;
using System.Collections.Generic;
using System.Data;
using Migrator.Framework;
using ForeignKeyConstraint=Migrator.Framework.ForeignKeyConstraint;
#if MOBILE
using SqliteConnection = Mono.Data.Sqlite.SqliteConnection;
#else
using SqliteConnection = System.Data.SQLite.SQLiteConnection;
#endif
using System.Diagnostics;
using System.Text.RegularExpressions;
using System.Linq;

namespace Migrator.Providers.SQLite
{
    /// <summary>
    /// Summary description for SQLiteTransformationProvider.
    /// </summary>
    public partial class SQLiteTransformationProvider : TransformationProvider
    {
        private readonly ForeignKeyConstraintMapper constraintMapper = new ForeignKeyConstraintMapper();
        private readonly string _wholeWordPattern = @"\b{0}\b";
        private const string _INDEX_TYPE = "Index";
        private const string _TRIGGER_TYPE = "Trigger";

        public SQLiteTransformationProvider(Dialect dialect, string connectionString)
            : base(dialect, connectionString)
        {
            _connection = GetConnection();
            _connection.ConnectionString = _connectionString;
            _connection.Open();

            EnableLegacyAlterTable();
        }

        private SqliteConnection GetConnection()
        {
            return new SqliteConnection(_connectionString);
        }

        private void EnableLegacyAlterTable()
        {
            // Enable legacy ALTER TABLE mode as the new way just requires other workarounds so is not really an improvement.
            // With the new way it seems we have to manually go in and update at least other triggers that reference a table
            // when we drop that table as part of recreating it (by creating a temp one, transferring data, dropping the original
            // and then renaming the temp one to the original name). If we don't do that then the next ALTER TABLE command
            // seems to do some kind of verification and find an error
            ExecuteNonQuery("PRAGMA legacy_alter_table = ON");
        }

        public override void AddTable(string name, string engine, params Column[] columns)
        {
            if (TableExists(name))
            {
                Logger.Warn("Table {0} already exists", name);
                return;
            }

            List<string> pks = GetPrimaryKeys(columns);
            bool compoundPrimaryKey = pks.Count > 1;

            List<ColumnPropertiesMapper> columnProviders = new List<ColumnPropertiesMapper>(columns.Length);
            foreach (Column column in columns)
            {
                // Remove the primary key notation if compound primary key because we'll add it back later
                if (compoundPrimaryKey && column.IsPrimaryKey)
                    column.ColumnProperty = ColumnProperty.Unsigned | ColumnProperty.NotNull;

                ColumnPropertiesMapper mapper = _dialect.GetAndMapColumnProperties(column);
                columnProviders.Add(mapper);
            }

            string columnsAndIndexes = JoinColumnsAndIndexes(columnProviders);
            AddTable(name, engine, columnsAndIndexes + (compoundPrimaryKey ? string.Format(", PRIMARY KEY ({0})", string.Join(",", pks)) : "")); // If this primary key string changes, fix the 'ParseSqlColumnDefs' method below to reflect it
        }

        public override void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable, string[] refColumns, ForeignKeyConstraint updateConstraint, ForeignKeyConstraint deleteConstraint)
        {
            if (primaryColumns.Length > 1 || refColumns.Length > 1)
            {
                Logger.Warn("Multiple columns in foreign key not supported");
                return;
            }

            // Generate new table definition with foreign key
            string compositeDefSql;
            string[] origColDefs = GetColumnDefs(primaryTable, out compositeDefSql);
            List<string> colDefs = new List<string>();
            foreach (string origdef in origColDefs)
            {
                // Is this column one we should add a foreign key to?
                if (ColumnMatch(primaryColumns[0], origdef))
                    colDefs.Add(origdef + GetForeignKeyConstraintDefinition(name, refTable, refColumns[0], updateConstraint, deleteConstraint));
                else
                    colDefs.Add(origdef);
            }

            string[] newColDefs = colDefs.ToArray();
            string colDefsSql = String.Join(",", newColDefs);

            string[] colNames = ParseSqlForColumnNames(newColDefs);
            string colNamesSql = String.Join(",", colNames);

            // Create new table with temporary name
            AddTable(primaryTable + "_temp", null, GetSqlForAddTable(primaryTable, colDefsSql, compositeDefSql));

            // Copy data from original table to temporary table
            ExecuteNonQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", primaryTable, colNamesSql));

            // Add indexes from original table
            MoveIndexesAndTriggersFromOriginalTable(primaryTable, primaryTable + "_temp");

            //PerformForeignKeyAffectedAction(() =>
            //{
                // Remove original table
                RemoveTable(primaryTable);

                // Rename temporary table to original table name
                ExecuteNonQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", primaryTable));
            //});
        }

        private string GetForeignKeyConstraintDefinition(string name, string refTable, string refColumn, ForeignKeyConstraint updateConstraint, ForeignKeyConstraint deleteConstraint)
        {
            return string.Format(" CONSTRAINT {0} REFERENCES {1}({2}) ON UPDATE {3} ON DELETE {4}", name, refTable, refColumn, constraintMapper.SqlForConstraint(updateConstraint), constraintMapper.SqlForConstraint(deleteConstraint));
        }

        public override void RemoveForeignKey(string table, string name)
        {
            throw new NotSupportedException("Use special SQLite method with more parameters instead.");
        }

        // Because our FK names haven't been unique, and you can have multiple keys for the same column, we match everything to ensure we remove the right one
        public void RemoveForeignKey(string name, string refTable, string refColumn, string primaryTable, string primaryColumn, ForeignKeyConstraint updateConstraint, ForeignKeyConstraint deleteConstraint, bool throwOnMissingKey = true)
        {
            // Generate new table definition with foreign key
            string compositeDefSql;
            string[] origColDefs = GetColumnDefs(primaryTable, out compositeDefSql);
            List<string> colDefs = new List<string>();

            var foreignKeyDef = GetForeignKeyConstraintDefinition(name, refTable, refColumn, updateConstraint, deleteConstraint);
            var foundMatch = false;
            foreach (string origdef in origColDefs)
            {
                // Strip the constraint part of the column definition
                if (ColumnMatch(primaryColumn, origdef) && origdef.Contains(foreignKeyDef))
                {
                    if (foundMatch)
                        throw new InvalidOperationException("Multiple foreign keys matching removal request."); // We likely never want to remove multiple keys in one go

                    colDefs.Add(origdef.Replace(foreignKeyDef, ""));
                    foundMatch = true;
                }
                else
                    colDefs.Add(origdef);
            }

            if (!foundMatch)
            {
                if (throwOnMissingKey)
                    throw new Exception("No foreign keys matching removal request."); // If this happens, it could be that the definition doesn't always match - which should be fixed
                else
                    return;
            }

            string[] newColDefs = colDefs.ToArray();
            string colDefsSql = String.Join(",", newColDefs);

            string[] colNames = ParseSqlForColumnNames(newColDefs);
            string colNamesSql = String.Join(",", colNames);

            // Create new table with temporary name
            AddTable(primaryTable + "_temp", null, GetSqlForAddTable(primaryTable, colDefsSql, compositeDefSql));

            // Copy data from original table to temporary table
            ExecuteNonQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", primaryTable, colNamesSql));

            // Add indexes from original table
            MoveIndexesAndTriggersFromOriginalTable(primaryTable, primaryTable + "_temp");

            //PerformForeignKeyAffectedAction(() =>
            //{
            // Remove original table
            RemoveTable(primaryTable);

            // Rename temporary table to original table name
            ExecuteNonQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", primaryTable));
            //});
        }

        private string GetSqlForAddTable(string tableName, string colDefsSql, string compositeDefSql)
        {
            return compositeDefSql != null ? colDefsSql.TrimEnd(')') + "," + compositeDefSql : colDefsSql;
        }

        private void MoveIndexesAndTriggersFromOriginalTable(string origTable, string newTable, string oldColumn = null, string newColumn = null)
        {
            MoveSpecialFromOriginalTable(_INDEX_TYPE, origTable, newTable, oldColumn, newColumn);
            MoveSpecialFromOriginalTable(_TRIGGER_TYPE, origTable, newTable, oldColumn, newColumn);
        }

        private void MoveSpecialFromOriginalTable(string type, string origTable, string newTable, string oldColumn, string newColumn)
        {
            foreach (var special in GetCreateSpecialDefs(origTable, type))
            {
                // First remove original special, because names have to be unique
                ExecuteNonQuery(string.Format("DROP {0} {1}", type.ToUpperInvariant(), special.Item1));

                // Create special on new table, replacing the table name.
                // Note for triggers we create on temp tables we only replace the name of the table it's created on (the first occurrence), not the name if defined in the actual trigger, 
                // since that won't auto-rename when we rename the table afterwards.
                var replaceRegex = new Regex(string.Format(_wholeWordPattern, origTable, RegexOptions.IgnoreCase));
                var createSql = replaceRegex.Replace(special.Item2, newTable, newTable.EndsWith("_temp") && type == _TRIGGER_TYPE ? 1 : int.MaxValue);

                Regex oldColumnRegex = new Regex(string.Format(_wholeWordPattern, oldColumn), RegexOptions.IgnoreCase);
                if (oldColumn != null && oldColumnRegex.IsMatch(createSql))
                {
                    // If the special is an index we want to rename it to follow the name of the column. We don't have a naming convention for triggers
                    if (type == _INDEX_TYPE)
                    {
                        // Make sure the index's name is following our conventions, so we can rename it automatically
                        var newIndexNameRegex = new Regex(string.Format(@"_{0}\b", oldColumn), RegexOptions.IgnoreCase);
                        if (!newIndexNameRegex.IsMatch(special.Item1))
                            throw new InvalidOperationException("Index name not following conventions. Instead remove and re-add index manually prior to renaming the column.");

                        // Rename index name to new column name
                        createSql = Regex.Replace(createSql, string.Format(_wholeWordPattern, special.Item1), newIndexNameRegex.Replace(special.Item1, "_" + newColumn), RegexOptions.IgnoreCase);
                    }
                    
                    // Rename any references to the old column
                    createSql = oldColumnRegex.Replace(createSql, newColumn);
                }

                ExecuteNonQuery(createSql);
            }
        }

        //private void PerformForeignKeyAffectedAction(Action action)
        //{
        //    // TODO: Technically this should check whether foreign keys are active
        //    ExecuteNonQuery("pragma foreign_keys = NO");
        //    action();
        //    ExecuteNonQuery("pragma foreign_keys = YES");
        //}

        public override void RemoveColumn(string table, string column)
        {
            if (! (TableExists(table) && ColumnExists(table, column)))
                return;

            string compositeDefSql;
            string[] origColDefs = GetColumnDefs(table, out compositeDefSql);
            List<string> colDefs = new List<string>();

            foreach (string origdef in origColDefs) 
            {
                if (! ColumnMatch(column, origdef))
                    colDefs.Add(origdef);
            }
            
            string[] newColDefs = colDefs.ToArray();
            string colDefsSql = String.Join(",", newColDefs);
             
            string[] colNames = ParseSqlForColumnNames(newColDefs);
            string colNamesSql = String.Join(",", colNames);

            AddTable(table + "_temp", null, GetSqlForAddTable(table, colDefsSql, compositeDefSql));
            ExecuteNonQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", table, colNamesSql));
            MoveIndexesAndTriggersFromOriginalTable(table, table + "_temp");
            //PerformForeignKeyAffectedAction(() =>
            //{
                RemoveTable(table);
                ExecuteNonQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", table));
            //});
        }
        
        public override void RenameColumn(string table, string oldColumn, string newColumn)
        {
            // NOTE renaming a column is now supported by a single command in SQLite, but it doesn't play nice with our
            // index naming check code so we keep using the old way for now.

            if (ColumnExists(table, newColumn))
                throw new MigrationException(String.Format("Table '{0}' has column named '{1}' already", table, newColumn));

            string compositeDefSql;
            string[] origColDefs = GetColumnDefs(table, out compositeDefSql);
            List<string> colDefs = new List<string>(origColDefs);

            string[] newColDefs = colDefs.ToArray();
            string colDefsSql = String.Join(",", newColDefs);

            string[] colNames = ParseSqlForColumnNames(newColDefs);
            string colNamesSql = String.Join(",", colNames);

            var regex = new Regex(string.Format(_wholeWordPattern, oldColumn), RegexOptions.IgnoreCase); // Doing a normal string.Replace would replace partial matches as well

            var addTableSql = regex.Replace(GetSqlForAddTable(table, colDefsSql, compositeDefSql), newColumn);
            AddTable(table + "_temp", null, addTableSql);
            ExecuteNonQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", table, colNamesSql)); // This was replacing {1} with regex.Replace(colNamesSql, oldColumn + " AS " + newColumn) but it would place AS between any quotes when it should be outside. But from what I can tell SQLite doesn't need AS at all so I removed it
            MoveIndexesAndTriggersFromOriginalTable(table, table + "_temp", oldColumn, newColumn);
            //PerformForeignKeyAffectedAction(() =>
            //{
                RemoveTable(table);
                ExecuteNonQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", table));
            //});


            //if (ColumnExists(tableName, newColumnName))
            //    throw new MigrationException(String.Format("Table '{0}' has column named '{1}' already", tableName, newColumnName));
                
            //if (ColumnExists(tableName, oldColumnName)) 
            //{
            //    string compositeDefSql;
            //    string[] columnDefs = GetColumnDefs(tableName, out compositeDefSql);
            //    string columnDef = Array.Find(columnDefs, delegate(string col) { return ColumnMatch(oldColumnName, col); });
                
            //    string newColumnDef = columnDef.Replace(oldColumnName, newColumnName);

            //    //// Not null columns needs a default
            //    //if (newColumnDef.Contains("NOT NULL"))
            //    //    newColumnDef += " DEFAULT ('')"; // SQLite can put any value into any column so we can d
                
            //    AddColumn(tableName, newColumnDef);
            //    ExecuteNonQuery(String.Format("UPDATE {0} SET {1}={2}", tableName, newColumnName, oldColumnName));
            //    RemoveColumn(tableName, oldColumnName);
            //}
        }
        
        public override void ChangeColumn(string table, Column column)
        {
            if (!ColumnExists(table, column.Name))
                throw new MigrationException(String.Format("Column '{0}.{1}' does not exist", table, column.Name));

            string compositeDefSql;
            string[] origColDefs = GetColumnDefs(table, out compositeDefSql);
            List<string> colDefs = new List<string>();

            foreach (string origdef in origColDefs) 
            {
                if (!ColumnMatch(column.Name, origdef))
                    colDefs.Add(origdef);
                else
                {
                    // If the original column had a constraint, we make sure to add it to the new definition
                    var constraint = "";
                    var constraintIndex = origdef.IndexOf("CONSTRAINT", StringComparison.OrdinalIgnoreCase);
                    if (constraintIndex > -1)
                        constraint = " " + origdef.Substring(constraintIndex);
                    
                    colDefs.Add(Dialect.GetAndMapColumnProperties(column).ColumnSql + constraint); // Add new column definition
                }
            }
            
            string[] newColDefs = colDefs.ToArray();
            string colDefsSql = String.Join(",", newColDefs);
             
            string[] colNames = ParseSqlForColumnNames(newColDefs);
            string colNamesSql = String.Join(",", colNames);

            AddTable(table + "_temp", null, GetSqlForAddTable(table, colDefsSql, compositeDefSql));
            ExecuteNonQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", table, colNamesSql));
            MoveIndexesAndTriggersFromOriginalTable(table, table + "_temp");
            //PerformForeignKeyAffectedAction(() =>
            //{
                RemoveTable(table);
                ExecuteNonQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", table));
            //});


            //if (! ColumnExists(table, column.Name))
            //{
            //    Logger.Warn("Column {0}.{1} does not exist", table, column.Name);
            //    return;
            //}

            //string tempColumn = "temp_" + column.Name;
            //RenameColumn(table, column.Name, tempColumn);
            //AddColumn(table, column);
            //ExecuteNonQuery(String.Format("UPDATE {0} SET {1}={2}", table, column.Name, tempColumn));
            //RemoveColumn(table, tempColumn);
        }

        public override bool TableExists(string table)
        {
            using (IDataReader reader =
                ExecuteQuery(String.Format("SELECT name FROM sqlite_master WHERE type='table' and name='{0}'",table)))
            {
                return reader.Read();
            }
        }
        
        public override bool ConstraintExists(string table, string name)
        {
            return false;
        }

        public override bool IndexExists(string table, string name)
		{
			using (IDataReader reader =
				ExecuteQuery(String.Format("SELECT name FROM sqlite_master WHERE type='index' and name='{0}'", name)))
			{
				return reader.Read();
			}
		}

        public override string[] GetTables()
        {
            List<string> tables = new List<string>();

            using (IDataReader reader = ExecuteQuery("SELECT name FROM sqlite_master WHERE type='table' AND name <> 'sqlite_sequence' ORDER BY name"))
            {
                while (reader.Read())
                {
                    tables.Add((string) reader[0]);
                }
            }

            return tables.ToArray();
        }

        public override Column GetColumnByName(string table, string columnName)
        {
            return Array.Find(GetColumns(table),
                delegate (Column column)
                {
                    return ColumnNameMatch(columnName, column.Name);
                });
        }

        public override Column[] GetColumns(string table)
        {       
            List<Column> columns = new List<Column>();
            string compositeDefSql;
            foreach (string columnDef in GetColumnDefs(table, out compositeDefSql))
            {
                string name = ExtractNameFromColumnDef(columnDef);
                Column column = new Column(name, ExtractTypeFromColumnDef(columnDef));

                bool isNullable = IsNullable(columnDef);
                column.ColumnProperty |= isNullable ? ColumnProperty.Null : ColumnProperty.NotNull;
                bool isUnique = IsUnique(columnDef);
                if (isUnique)
                    column.ColumnProperty |= ColumnProperty.Unique;

                columns.Add(column);
            }
            return columns.ToArray();
        }

        private string GetSqlDefString(string table) 
        {
            string sqldef = null;

            using (IDataReader reader = ExecuteQuery(String.Format("SELECT sql FROM sqlite_master WHERE type='table' AND name='{0}'",table)))
                if (reader.Read())
                    sqldef = (string) reader[0];

            return sqldef;    
        }

        private Tuple<string, string>[] GetCreateSpecialDefs(string table, string type)
        {
            var sqlStrings = new List<Tuple<string, string>>();

            using (IDataReader reader = ExecuteQuery(String.Format("SELECT name, sql FROM sqlite_master WHERE type='{0}' AND sql NOT NULL AND tbl_name='{1}'", type.ToLowerInvariant(), table)))
                while (reader.Read())
                    sqlStrings.Add(new Tuple<string, string>((string)reader["name"], (string)reader["sql"]));

            return sqlStrings.ToArray();
        } 

        private string[] GetColumnNames(string table)
        {
            string compositeDefSql;
            return ParseSqlForColumnNames(GetSqlDefString(table), out compositeDefSql);
        }

        private string[] GetColumnDefs(string table, out string compositeDefSql)
        {
           return ParseSqlColumnDefs(GetSqlDefString(table), out compositeDefSql);
        }

        /// <summary>
        /// Turn something like 'columnName INTEGER NOT NULL' into just 'columnName'
        /// </summary>
        private string[] ParseSqlForColumnNames(string sqldef, out string compositeDefSql) 
        {
            string[] parts = ParseSqlColumnDefs(sqldef, out compositeDefSql);
            return ParseSqlForColumnNames(parts);
        }

        private string[] ParseSqlForColumnNames(string[] parts) 
        {
            if (null == parts)
                return null;
                
            for (int i = 0; i < parts.Length; i ++) 
            {
                parts[i] = ExtractNameFromColumnDef(parts[i]);
            }
            return parts;
        }

        /// <summary>
        /// Name is the first value before the space.
        /// </summary>
        /// <param name="columnDef"></param>
        /// <returns></returns>
        private string ExtractNameFromColumnDef(string columnDef)
        {
            int idx = columnDef.IndexOf(" ");
            if (idx > 0)
            {
                return columnDef.Substring(0, idx);
            }
            return null;
        }

        private DbType ExtractTypeFromColumnDef(string columnDef)
        {
            int idx = columnDef.IndexOf(" ") + 1;
            if (idx > 0)
            {
                var idy = columnDef.IndexOf(" ", idx) - idx;

                if (idy > 0)
                    return _dialect.GetDbType(columnDef.Substring(idx, idy));
                else
                    return _dialect.GetDbType(columnDef.Substring(idx));
            }
            else
                throw new Exception("Error extracting type from column definition: '" + columnDef + "'");
        }

        private bool IsNullable(string columnDef)
        {
            return ! columnDef.Contains("NOT NULL");
        }

        private bool IsUnique(string columnDef)
        {
            return columnDef.Contains(" UNIQUE "); // Need the spaces. Could be 'UNIQUEIDENTIFIER'
        }

        private string[] ParseSqlColumnDefs(string sqldef, out string compositeDefSql) 
        {
            if (String.IsNullOrEmpty(sqldef)) 
            {
                compositeDefSql = null;
                return null;
            }
            
            sqldef = sqldef.Replace(Environment.NewLine, " ");
            int start = sqldef.IndexOf("(");

            // Code to handle composite primary keys /mol
            int compositeDefIndex = sqldef.IndexOf("PRIMARY KEY ("); // Not ideal to search for a string like this but I'm lazy
            if (compositeDefIndex > -1)
            {
                compositeDefSql = sqldef.Substring(compositeDefIndex, sqldef.LastIndexOf(")") - compositeDefIndex);
                sqldef = sqldef.Substring(0, compositeDefIndex).TrimEnd(',', ' ') + ")";
            }
            else
                compositeDefSql = null;
            
            int end = sqldef.LastIndexOf(")"); // Changed from 'IndexOf' to 'LastIndexOf' to handle foreign key definitions /mol
            
            sqldef = sqldef.Substring(0, end);
            sqldef = sqldef.Substring(start + 1);
            
            string[] cols = sqldef.Split(new char[]{','});
            for (int i = 0; i < cols.Length; i ++) 
            {
                cols[i] = cols[i].Trim();
            }
            return cols;
        }

        private bool ColumnNameMatch(string columnName, string columnDefName)
        {
            return ColumnMatch(columnName, columnDefName + " "); // Adding a space because we check for that in the method. Slightly hack'ish
        }

        private bool ColumnMatch(string columnName, string columnDef)
        {
            // Note when comparing the names outright we add a space because we check for that above when doing a name match.
            // Since the quotes surround the name otherwise we don't need to include that when matching with quotes
            return columnDef.StartsWith(columnName + " ") || _dialect.QuoteTemplates.Any(t => columnDef.StartsWith(string.Format(t, columnName)));
        }

        public override void MigrationApplied(long version)
        {
#if DEBUG
            // If connection state is not open, something went wrong and we don't need to check for violations
            if (_connection.State == ConnectionState.Open)
            {
                // If we're debugging, check to make sure the migration didn't cause foreign key violations.
                // The reason we only check when debugging is that the check can take a few seconds, and could also reveal earlier violations that might not cause issues for users
                var hasViolation = HasForeignKeyViolation();

                if (!hasViolation)
                    hasViolation = HasNamingViolation();

                if (hasViolation)
                    throw new Exception($"Database violation detected. Migration: {version}");
            }
#endif

            base.MigrationApplied(version);
        }

        private bool HasForeignKeyViolation()
        {
            return ExecuteQuery("PRAGMA foreign_key_check").Read();
        }

        private bool HasNamingViolation()
        {
            // Check that all naming conventions are being followed, since for indexes it's important in order for the migrator to work properly
            using (var masterReader = ExecuteQuery("SELECT type, name, tbl_name FROM sqlite_master WHERE (type = 'index' OR type = 'trigger') AND sql NOT NULL"))
            {
                while (masterReader.Read())
                {
                    var name = (string)masterReader["name"];
                    var table = (string)masterReader["tbl_name"];

                    switch (masterReader["type"])
                    {
                        case "index":
                            // Get index column names
                            var columns = new List<string>();
                            using (var indexReader = ExecuteQuery($"PRAGMA index_info({name})"))
                                while (indexReader.Read())
                                    columns.Add((string)indexReader["name"]);

                            if (name != $"IN_{table}_{string.Join("_", columns)}")
                                return true;

                            break;

                        case "trigger":
                            if (!name.StartsWith($"TR_{table}_"))
                                return true;

                            break;

                        default:
                            throw new NotImplementedException();
                    }
                }
            }

            return false;
        }
    }
}