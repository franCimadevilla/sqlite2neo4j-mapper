import logging
from abc import ABC, abstractmethod
from neo4j import GraphDatabase
from .schema_reader import SchemaReader
from .populate_graph import SQL2GraphMapper

logger = logging.getLogger(__name__)

class AbstractSQL2Neo4jPipeline(ABC):
    def __init__(
        self,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        relations_map: dict | None = None,
        dry_run: bool = False,
    ):
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.relations_map = relations_map or {}
        self.dry_run = dry_run

    @abstractmethod
    def _get_db_type(self) -> str:
        pass

    @abstractmethod
    def _get_db_config(self) -> dict:
        pass

    def run(self):
        logger.info(f"Starting {self.__class__.__name__} pipeline")

        db_type = self._get_db_type()
        db_config = self._get_db_config()

        logger.info(f"Reading from {db_type} the db schema")
        schema_reader = SchemaReader(db_type=db_type, **db_config)
        schema = schema_reader.extract_schema()

        logger.info(f"{len(schema)} tables detected")

        if self.dry_run:
            logger.warning("DRY RUN enabled — no data will be written to Neo4j")
            print(schema)
            return schema

        logger.info("Connecting to Neo4j")
        driver = GraphDatabase.driver(
            self.neo4j_uri,
            auth=(self.neo4j_user, self.neo4j_password),
        )

        mapper = SQL2GraphMapper(
            schema=schema,
            db_type=db_type,
            db_config=db_config,
            db_driver=driver,
            relations_map=self.relations_map,
        )

        mapper.populate_db()
        driver.close()
        logger.info("Pipeline finished successfully")

class LocalSQL2Neo4jPipeline(AbstractSQL2Neo4jPipeline):
    def __init__(
        self,
        sqlite_path: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        relations_map: dict | None = None,
        dry_run: bool = False,
    ):
        super().__init__(
            neo4j_uri, neo4j_user, neo4j_password, relations_map, dry_run
        )
        self.sqlite_path = sqlite_path

    def _get_db_type(self) -> str:
        return "sqlite"

    def _get_db_config(self) -> dict:
        return {"db_path": self.sqlite_path}

class RemoteSQL2Neo4jPipeline(AbstractSQL2Neo4jPipeline):
    def __init__(
        self,
        sql_server_host: str,
        sql_server_user: str,
        sql_server_password: str,
        sql_server_database: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        relations_map: dict | None = None,
        dry_run: bool = False,
        sql_server_port: int = 3306,
    ):
        super().__init__(
            neo4j_uri, neo4j_user, neo4j_password, relations_map, dry_run
        )
        self.sql_server_host = sql_server_host
        self.sql_server_user = sql_server_user
        self.sql_server_password = sql_server_password
        self.sql_server_database = sql_server_database
        self.sql_server_port = sql_server_port

    def _get_db_type(self) -> str:
        return "mariadb"

    def _get_db_config(self) -> dict:
        return {
            "host": self.sql_server_host,
            "user": self.sql_server_user,
            "password": self.sql_server_password,
            "database": self.sql_server_database,
            "port": self.sql_server_port,
        }
