import logging
from neo4j import GraphDatabase
from .schema_reader import SchemaReader
from .populate_graph import SQL2GraphMapper

logger = logging.getLogger(__name__)


class SQL2Neo4jPipeline:
    def __init__(
        self,
        sqlite_path: str,
        neo4j_uri: str,
        neo4j_user: str,
        neo4j_password: str,
        relations_map: dict | None = None,
        dry_run: bool = False,
    ):
        self.sqlite_path = sqlite_path
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.relations_map = relations_map or {}
        self.dry_run = dry_run

    def run(self):
        logger.info("Starting SQL → Neo4j pipeline")

        logger.info("Reading SQLite schema")
        schema_reader = SchemaReader(self.sqlite_path)
        schema = schema_reader.extract_schema()

        logger.info(f"{len(schema)} tables detected")

        if self.dry_run:
            logger.warning("DRY RUN enabled — no data will be written to Neo4j")
            return schema

        logger.info("Connecting to Neo4j")
        driver = GraphDatabase.driver(
            self.neo4j_uri,
            auth=(self.neo4j_user, self.neo4j_password),
        )

        mapper = SQL2GraphMapper(
            schema=schema,
            db_path=self.sqlite_path,
            db_driver=driver,
            relations_map=self.relations_map,
        )

        mapper.populate_db()
        logger.info("Pipeline finished successfully")
