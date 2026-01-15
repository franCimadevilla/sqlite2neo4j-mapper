from sql2graph.logging_config import setup_logging
from sql2graph.pipeline import SQLToNeo4jPipeline

def main():
    """
        Allows execute the lib funcionality as a program. 
        Shows an example of implementation of the pipeline.
    """
    setup_logging()

    pipeline = SQLToNeo4jPipeline(
        sqlite_path="data/olist.sqlite",
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="neo4jPasswd",
        relations_map={
            "user_id": "BELONGS_TO_USER",
            "order_id": "HAS_ORDER",
            "product_id": "CONTAINS_PRODUCT",
        },
        dry_run=False,
    )

    pipeline.run()


if __name__ == "__main__":
    main()
