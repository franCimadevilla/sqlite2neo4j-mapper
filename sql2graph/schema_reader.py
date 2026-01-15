import sqlite3
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class SchemaReader():
    def __init__(self, db_path=None):
        self.db_path = db_path
    
    def extract_schema(self) -> dict:
        """
        Extracts the schema meaningful structures of data
        """        

        conn = sqlite3.connect(self.db_path)
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
