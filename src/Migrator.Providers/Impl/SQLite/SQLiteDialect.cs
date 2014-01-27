
using System;
using System.Data;
using Migrator.Framework;

namespace Migrator.Providers.SQLite
{
	public class SQLiteDialect : Dialect
	{
	    public SQLiteDialect()
	    {
            // SQLite doesn't operate with a rigid type system, but supplying the proper types helps Dapper map correctly

	        RegisterColumnType(DbType.Binary, "BINARY");
            RegisterColumnType(DbType.Byte, "TINYINT");
            RegisterColumnType(DbType.Int16, "SMALLINT");
            RegisterColumnType(DbType.Int32, "INTEGER");
            RegisterColumnType(DbType.Int64, "BIGINT");
            RegisterColumnType(DbType.SByte, "INTEGER");
            RegisterColumnType(DbType.UInt16, "INTEGER");
            RegisterColumnType(DbType.UInt32, "INTEGER");
            RegisterColumnType(DbType.UInt64, "INTEGER");

            RegisterColumnType(DbType.Currency, "CURRENCY");
            RegisterColumnType(DbType.Decimal, "DECIMAL");
            RegisterColumnType(DbType.Double, "DOUBLE");
            RegisterColumnType(DbType.Single, "REAL");
            RegisterColumnType(DbType.VarNumeric, "NUMERIC");

            RegisterColumnType(DbType.String, "TEXT");
            RegisterColumnType(DbType.StringFixedLength, "TEXT");
            RegisterColumnType(DbType.AnsiString, "TEXT");
            RegisterColumnType(DbType.AnsiStringFixedLength, "TEXT");

            RegisterColumnType(DbType.Date, "DATE");
            RegisterColumnType(DbType.DateTime, "DATETIME");
            RegisterColumnType(DbType.Time, "TIME");
            RegisterColumnType(DbType.Boolean, "BOOLEAN"); // Important for Dapper to know it should map to a bool
            RegisterColumnType(DbType.Guid, "UNIQUEIDENTIFIER");
            
            RegisterProperty(ColumnProperty.Identity, "AUTOINCREMENT");
        }

        public override Type TransformationProvider { get { return typeof(SQLiteTransformationProvider); } }
        
        public override bool NeedsNotNullForIdentity
        {
            get { return true; }
        }
    }
}