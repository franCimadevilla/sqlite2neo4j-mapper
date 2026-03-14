import sqlite3
from typing import Protocol
import logging

class SQL2GraphMapper():
    """SQL Mapper to a Graph database. Constrained to Neo4j Community edition database driver, that has only one graph per database. 
    """
    def __init__(self, schema : dict, db_type : str = "sqlite", db_config : dict | None = None, db_driver=None|Protocol, relations_map : dict|None = None):
        if db_driver == None:
            raise Exception(f"Database driver cannot be None.")
    
        self.driver = db_driver
        self.schema = schema
        self.db_type = db_type
        self.db_config = db_config or {}
        self.relations_map = relations_map or {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_connection(self):
        if self.db_type == "sqlite":
            return sqlite3.connect(self.db_config.get("db_path"))
        elif self.db_type == "mariadb":
            try:
                import mysql.connector
            except ImportError:
                raise ImportError("mysql-connector-python is required for MariaDB support")
            return mysql.connector.connect(**self.db_config)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def populate_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        self.logger.info("Starting DB population")
        
        with self.driver.session() as session:  # Session scope for neo4j db

            # Clear graph
            self.logger.debug("Clearing graph")
            session.run("MATCH (n) DETACH DELETE n")

            # Create constraints to optimize
            self.logger.info("Creating constraints")
            self._create_constraints(session)    

            for table_name, details in self.schema.items():
                self.logger.info(f"Processing table: {table_name}")

                ## 1. Add nodes 
                cursor.execute(f"SELECT * FROM {table_name}; ")
                rows = cursor.fetchall()
                self.logger.debug(f"{table_name}: {len(rows)} rows")

                self._batch_insert_nodes(session, table_name, details, rows)
                
                ## 2. Add relationships
                for fk in details["FKs"]:
                    rel_type = self._map_relationships(fk)
                    self.logger.debug(
                        f"Creating relationships {table_name} -> {fk['ref_table']} [{rel_type}]"
                    )

                    cursor.execute(
                        f"SELECT {fk['column']}, {fk['ref_column']} FROM {table_name}"
                    )
                    relations = cursor.fetchall()

                    for a_val, b_val in relations:
                        try:
                            session.run(
                                f"""
                                MATCH (a:{table_name} {{{fk['column']}: $a}})
                                MATCH (b:{fk['ref_table']} {{{fk['ref_column']}: $b}})
                                MERGE (a)-[:{rel_type}]->(b)
                                """,
                                a=a_val,
                                b=b_val
                            )
                        except Exception as e:
                            self.logger.error(
                                f"Failed relationship {table_name}.{fk['column']} -> "
                                f"{fk['ref_table']}.{fk['ref_column']} "
                                f"({a_val} -> {b_val})",
                                exc_info=e
                            )
            
            #end outter for scope
            conn.close()
        #end with scope
        self.logger.info("DB population finished")

    def _create_constraints(self, session):
        for table, details in self.schema.items():
            pk_cols = details.get("PKs", [])

            if len(pk_cols) == 1:
                pk = pk_cols[0]
                self.logger.info(
                    f"Creating UNIQUE constraint on :{table}({pk})"
                )
                session.run(
                    f"""
                    CREATE CONSTRAINT IF NOT EXISTS
                    FOR (n:{table})
                    REQUIRE n.{pk} IS UNIQUE
                    """
                )
            elif len(pk_cols) > 1:
                self.logger.warning(
                    f"Composite PK detected on table {table}: {pk_cols}. "
                    f"No UNIQUE constraint created."
                )

    def _batch_insert_nodes(self, session, table_name, details, rows, batch_size=1000):
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i+batch_size]

            batch = [
                {
                    col["name"]: self._cast_value(row[idx])
                    for idx, col in enumerate(details["columns"])
                }
                for row in chunk
            ]

            session.run(
                f"""
                UNWIND $batch AS row
                MERGE (n:{table_name} {{ {self._pk_match(details)} }})
                SET n += row
                """,
                batch=batch
            )

            self.logger.debug(
                f"{table_name}: inserted {i + len(chunk)}/{len(rows)}"
            )


    def _pk_match(self, details):
        pk_cols = details.get("PKs", [])

        if len(pk_cols) == 1:
            pk = pk_cols[0]
            return f"{pk}: row.{pk}"
        else:
            # fallback: merge of all properties (less efficient)
            return ", ".join(
                f"{c['name']}: row.{c['name']}"
                for c in details["columns"]
            )


    def _cast_value(self, value):
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def _map_relationships(self, fk):
        col = fk["column"].lower()
        
        return self.relations_map.get(col, col).upper()
