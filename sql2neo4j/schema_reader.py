import sqlite3
import logging

logger = logging.getLogger(__name__)

class SchemaReader:
    def __init__(self, db_type="sqlite", **db_config):
        self.db_type = db_type
        self.db_config = db_config
    
    def extract_schema(self) -> dict:
        if self.db_type == "sqlite":
            return self._extract_sqlite_schema()
        elif self.db_type == "mariadb":
            return self._extract_mariadb_schema()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _extract_sqlite_schema(self) -> dict:
        db_path = self.db_config.get("db_path")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # table definitions
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'; ")
        tables = cursor.fetchall()
        logger.debug(f"numTables: {len(tables)}, tableNames: {[str(name) for name, _ in tables]}")

        schema = dict()
        for table_name, table_sql in tables:
            schema[table_name] = {"columns": [], "PKs": [], "FKs": []}

            # Get column details
            cursor.execute(f"PRAGMA table_info('{table_name}'); ")
            columns = cursor.fetchall()

            for column in columns: 
                column_name = column[1]
                column_type = column[2]
                is_pk = column[5] == 1
                
                logger.debug(f"For table: {table_name}, column: {column_name}, is_pk: {is_pk}")
                schema[table_name]["columns"].append(
                    {"name": column_name, "type": column_type, "is_pk": is_pk}
                )
                if is_pk:
                    schema[table_name]["PKs"].append(column_name)
                
            # Get foreign key details
            cursor.execute(f"PRAGMA foreign_key_list('{table_name}'); ")
            foreign_keys = cursor.fetchall()
            for fk in foreign_keys:
                schema[table_name]["FKs"].append({
                        "column": fk[3],
                        "ref_table": fk[2],
                        "ref_column": fk[4]
                    })

        conn.close()
        return schema

    def _extract_mariadb_schema(self) -> dict:
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("mysql-connector-python is required for MariaDB support")

        conn = mysql.connector.connect(**self.db_config)
        cursor = conn.cursor(dictionary=True)
        db_name = self.db_config.get("database")

        # Get tables
        cursor.execute(f"""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{db_name}'
        """)
        tables = cursor.fetchall()
        
        schema = dict()
        for table in tables:
            table_name = table['TABLE_NAME']
            schema[table_name] = {"columns": [], "PKs": [], "FKs": []}

            # Get columns and PK info
            cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{table_name}'
            """)
            columns = cursor.fetchall()
            for col in columns:
                col_name = col['COLUMN_NAME']
                is_pk = col['COLUMN_KEY'] == 'PRI'
                schema[table_name]["columns"].append({
                    "name": col_name,
                    "type": col['DATA_TYPE'],
                    "is_pk": is_pk
                })
                if is_pk:
                    schema[table_name]["PKs"].append(col_name)

            # Get FK info
            cursor.execute(f"""
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME = '{table_name}'
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """)
            fks = cursor.fetchall()
            for fk in fks:
                schema[table_name]["FKs"].append({
                    "column": fk['COLUMN_NAME'],
                    "ref_table": fk['REFERENCED_TABLE_NAME'],
                    "ref_column": fk['REFERENCED_COLUMN_NAME']
                })

        conn.close()
        return schema
