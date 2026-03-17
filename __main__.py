from sql2neo4j.logging_config import setup_logging
from sql2neo4j.pipeline import LocalSQL2Neo4jPipeline, RemoteSQL2Neo4jPipeline


def main():
    """
        Allows execute the lib funcionality as a program. 
        Shows an example of implementation of the pipeline.
    """
    setup_logging()

    #local_database_process()

    remote_database_process()


def remote_database_process():
    # Pipeline invoke for Remote Server
    pipeline = RemoteSQL2Neo4jPipeline(
        sql_server_host="relational.fel.cvut.cz",
        sql_server_user="guest",
        sql_server_password="ctu-relational",
        sql_server_database="CORA",
        neo4j_uri="bolt://localhost:7687",
        neo4j_user="neo4j",
        neo4j_password="neo4jPasswd",
        dry_run=False,
        relations_map={
            "paper_id":"HAS_WORD",
            "citing_paper_id":"CITES",
            "cited_paper_id":"CITED_BY"
        },
    )

    pipeline.run()


def local_database_process():
    # Pipeline invoke for Local File

    pipeline = LocalSQL2Neo4jPipeline(
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
