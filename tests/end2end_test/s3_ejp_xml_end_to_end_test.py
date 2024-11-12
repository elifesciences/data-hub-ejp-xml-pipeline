import logging
from ejp_xml_pipeline.cli import main

LOGGER = logging.getLogger(__name__)


def test_ejp_xml_pipeline_cli():
    LOGGER.info("Running end2end test for eJP XML pipeline with sample config")
    main()
