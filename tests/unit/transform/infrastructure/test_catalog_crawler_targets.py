from aws_cdk import App
from aws_cdk.assertions import Template

from infrastructure.core.shared_storage_stack import SharedStorageStack
from infrastructure.governance.catalog_stack import DataCatalogStack


def _base_config():
    return {
        "s3_retention_days": 30,
        "auto_delete_objects": True,
        "log_retention_days": 14,
        "ingestion_domain": "market",
        "ingestion_table_name": "prices",
        "processing_triggers": [
            {
                "domain": "market",
                "table_name": "prices",
                "file_type": "json",
                "suffixes": [".json"],
            },
            {
                "domain": "sales",
                "table_name": "orders",
                "file_type": "csv",
                "suffixes": [".csv"],
            },
        ],
    }


def test_crawler_targets_scoped_and_recrawl_policy() -> None:
    app = App()
    cfg = _base_config()
    shared = SharedStorageStack(app, "SharedStorage4", environment="dev", config=cfg)
    catalog = DataCatalogStack(
        app,
        "CatalogStackTest",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
    )

    template = Template.from_stack(catalog)
    crawlers = template.find_resources("AWS::Glue::Crawler")
    assert crawlers, "Crawler must be defined"
    crawler = next(iter(crawlers.values()))

    # Targets must include domain/table subpaths
    s3_targets = crawler["Properties"]["Targets"]["S3Targets"]
    assert any("market/prices/" in t.get("Path", "") for t in s3_targets)
    assert any("sales/orders/" in t.get("Path", "") for t in s3_targets)

    # Recrawl policy should be new-folders only
    assert crawler["Properties"]["RecrawlPolicy"]["RecrawlBehavior"] == "CRAWL_NEW_FOLDERS_ONLY"
