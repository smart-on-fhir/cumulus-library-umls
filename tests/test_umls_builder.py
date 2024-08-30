import os
from unittest import mock

import pytest
import responses
from cumulus_library import base_utils, databases, db_config, study_manifest

from cumulus_library_umls import umls_builder

AUTH_URL = "https://utslogin.nlm.nih.gov/validateUser"
RELEASE_URL = "https://uts-ws.nlm.nih.gov/releases"
DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"


@pytest.fixture
def mock_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:
        with open("./tests/test_data/2000AA.zip", "rb") as download_zip:
            response.add(
                responses.GET,
                AUTH_URL,
                body="true",
                status=200,
                content_type="application/json",
            )
            response.add(
                responses.GET,
                RELEASE_URL,
                body="""[{
                    "fileName": "2000AA.zip",
                    "releaseVersion": "2000AA",
                    "releaseDate": "2000-01-01",
                    "downloadUrl": "https://download.nlm.nih.gov/umls/kss/2000AA/2000AA.zip",
                    "releaseType": "UMLS Metathesaurus Level 0 Subset",
                    "product": "UMLS",
                    "current": true
                    }]""",
                status=200,
                content_type="application/json",
            )
            response.add(
                responses.GET,
                DOWNLOAD_URL,
                body=download_zip.read(),
                status=200,
                content_type="application/zip",
            )
            yield response


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pathlib.Path.resolve")
def test_create_query(mock_resolve, mock_responses, tmp_path):
    mock_loc = tmp_path / "umls_builder.py"
    mock_resolve.return_value = mock_loc

    db_config.db_type = "duckdb"
    config = base_utils.StudyConfig(
        db=databases.DuckDatabaseBackend(f"{tmp_path}/duckdb"),
        umls_key="123",
        schema="main",
    )
    builder = umls_builder.UMLSBuilder()
    manifest = study_manifest.StudyManifest()
    builder.prepare_queries(config=config, manifest=manifest)
    expected = f"""CREATE TABLE IF NOT EXISTS umls__TESTTABLE AS SELECT
    TTY,
    CODE
FROM read_parquet('{
        tmp_path / "generated_parquet/2000AA"
    }/TESTTABLE.parquet/*.parquet')"""
    assert expected == builder.queries[0]


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pathlib.Path.resolve")
def test_create_query_download_exists(mock_resolve, mock_responses, tmp_path):
    mock_loc = tmp_path / "umls_builder.py"
    mock_resolve.return_value = mock_loc

    prev_download_path = tmp_path / "downloads/1999AA/"
    prev_download_path.mkdir(exist_ok=True, parents=True)
    prev_parquet_path = tmp_path / "generated_parquet/1999AA/"
    prev_parquet_path.mkdir(exist_ok=True, parents=True)

    db_config.db_type = "duckdb"
    config = base_utils.StudyConfig(
        db=databases.DuckDatabaseBackend(f"{tmp_path}/duckdb"),
        umls_key="123",
        schema="main",
    )
    builder = umls_builder.UMLSBuilder()
    manifest = study_manifest.StudyManifest()
    builder.prepare_queries(config=config, manifest=manifest)
    download_dirs = sorted((tmp_path / "downloads").iterdir())
    assert len(download_dirs) == 1
    assert "2000AA" in str(download_dirs[0])
    parquet_dirs = sorted((tmp_path / "generated_parquet").iterdir())
    assert len(parquet_dirs) == 1
    assert "2000AA" in str(parquet_dirs[0])
