import os
from unittest import mock

import pytest
import responses
from cumulus_library import base_utils, databases, db_config, study_manifest

from rxnorm import rxnorm_builder

AUTH_URL = "https://utslogin.nlm.nih.gov/validateUser"
RELEASE_URL = "https://uts-ws.nlm.nih.gov/releases"
DOWNLOAD_URL = "https://uts-ws.nlm.nih.gov/download"


@pytest.fixture
def mock_responses():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as response:
        with open("./tests/test_data/RxNorm_full_01012000.zip", "rb") as download_zip:
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
                    "fileName": "RxNorm_full_01012000.zip",
                    "releaseVersion": "2000-01-01",
                    "releaseDate": "2000-01-01",
                    "downloadUrl": "https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_01012020.zip",
                    "releaseType": "RxNorm Full Monthly Release",
                    "product": "RxNorm",
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
    mock_loc = tmp_path / "rxnorm_builder.py"
    mock_resolve.return_value = mock_loc

    db_config.db_type = "duckdb"
    config = base_utils.StudyConfig(
        db=databases.DuckDatabaseBackend(f"{tmp_path}/duckdb"), umls_key="123", schema='test'
    )
    manifest = study_manifest.StudyManifest()
    manifest._study_config = {
        'study_prefix': 'rxnorm',  
        'advanced_options': {'dedicated_schema': 'rxnorm'}
    }
    builder = rxnorm_builder.RxNormBuilder()
    builder.prepare_queries(config=config, manifest=manifest)
    print(builder.queries)
    expected = f"""CREATE TABLE IF NOT EXISTS rxnorm__TESTTABLE AS SELECT
    TTY,
    CODE
FROM read_parquet('{
        tmp_path / "generated_parquet/2000-01-01"
    }/TESTTABLE/*.parquet')"""
    assert expected == builder.queries[0]


@mock.patch.dict(
    os.environ,
    clear=True,
)
@mock.patch("pathlib.Path.resolve")
def test_create_query_download_exists(mock_resolve, mock_responses, tmp_path):
    mock_loc = tmp_path / "rxnorm_builder.py"
    mock_resolve.return_value = mock_loc

    prev_download_path = tmp_path / "downloads/1999AA/"
    prev_download_path.mkdir(exist_ok=True, parents=True)
    prev_parquet_path = tmp_path / "generated_parquet/1999AA/"
    prev_parquet_path.mkdir(exist_ok=True, parents=True)

    db_config.db_type = "duckdb"
    config = base_utils.StudyConfig(
        db=databases.DuckDatabaseBackend(f"{tmp_path}/duckdb"), umls_key="123", schema='test'
    )
    manifest = study_manifest.StudyManifest()
    manifest._study_config = {
        'study_prefix': 'rxnorm',  
        'advanced_options': {'dedicated_schema': 'rxnorm'}
    }
    builder = rxnorm_builder.RxNormBuilder()
    builder.prepare_queries(config=config, manifest=manifest)
    download_dirs = sorted((tmp_path / "downloads").iterdir())
    assert len(download_dirs) == 1
    assert "2000-01-01" in str(download_dirs[0])
    parquet_dirs = sorted((tmp_path / "generated_parquet").iterdir())
    assert len(parquet_dirs) == 1
    assert "2000-01-01" in str(parquet_dirs[0])
