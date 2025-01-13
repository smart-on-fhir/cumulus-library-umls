import os
from unittest import mock

import pytest
import responses
from cumulus_library import base_utils, databases, db_config, study_manifest
from cumulus_library.builders import protected_table_builder

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
        schema="umls",
    )
    config.db.connect()
    cursor = config.db.cursor()
    cursor.execute("CREATE SCHEMA umls")
    manifest = study_manifest.StudyManifest(study_path="./cumulus_library_umls/")
    p_builder = protected_table_builder.ProtectedTableBuilder()
    p_builder.execute_queries(config=config, manifest=manifest)
    builder = umls_builder.UMLSBuilder()
    builder.execute_queries(config=config, manifest=manifest)
    res = cursor.execute('SELECT * FROM "umls.TESTTABLE"').fetchall()
    assert res == [("TTY1", "Code-1"), ("TTY2", "Code-2"), ("TTY3", "Code-3")]


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
    config.db.connect()
    cursor = config.db.cursor()
    cursor.execute("CREATE SCHEMA umls")
    manifest = study_manifest.StudyManifest(study_path="./cumulus_library_umls/")
    p_builder = protected_table_builder.ProtectedTableBuilder()
    p_builder.execute_queries(config=config, manifest=manifest)
    builder = umls_builder.UMLSBuilder()
    builder.prepare_queries(config=config, manifest=manifest)
    download_dirs = sorted((tmp_path / "downloads").iterdir())
    assert len(download_dirs) == 1
    assert "2000AA" in str(download_dirs[0])
    parquet_dirs = sorted((tmp_path / "generated_parquet").iterdir())
    assert len(parquet_dirs) == 1
    assert "2000AA" in str(parquet_dirs[0])
