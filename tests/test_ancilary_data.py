import pathlib

from cumulus_library import base_utils, databases, db_config

from cumulus_library_umls import umls_builder


def test_umls_tree(tmp_path):
    db_config.db_type = "duckdb"
    builder = umls_builder.UMLSBuilder()
    config = base_utils.StudyConfig(
        db=databases.DuckDatabaseBackend(f"{tmp_path}/duckdb"),
        umls_key="123",
        schema="main",
    )
    config.db.connect()
    cursor = config.db.cursor()
    for file, columns in [
        (
            "MRCONSO",
            [
                "CUI",
                "LAT",
                "TS",
                "LUI",
                "STT",
                "SUI",
                "ISPREF",
                "AUI",
                "SAUI",
                "SCUI",
                "SDUI",
                "SAB",
                "TTY",
                "CODE",
                "STR",
                "SRL",
                "SUPPRESS",
                "CVF",
            ],
        ),
        (
            "MRREL",
            [
                "CUI1",
                "AUI1",
                "STYPE1",
                "REL",
                "CUI2",
                "AUI2",
                "STYPE2",
                "RELA",
                "RUI",
                "SRUI",
                "SAB",
                "SL",
                "RG",
                "DIR",
                "SUPPRESS",
                "CVF",
            ],
        ),
    ]:
        filepath = pathlib.Path.cwd() / "tests/test_data/2000AA/META/"
        with open(filepath / f"{file}.ctl") as f:
            datasource, table = builder.parse_ctl_file(f.readlines())
            rrf_path = filepath / f"{file}.RRF"
            builder.create_parquet(rrf_path, tmp_path, table)
            cursor.execute(
                f"""CREATE TABLE "umls__{file}" AS SELECT "{'","'.join(columns)}"
            FROM read_parquet('{tmp_path}/{file}/{file}.parquet')"""
            )

    with open("./cumulus_library_umls/ancilary_tables.sql") as f:
        queries = f.read().split(";")
        for query in queries:
            cursor.execute(query)
    tree = cursor.execute("SELECT  * FROM umls__icd10_tree ORDER BY rui ASC").fetchall()
    assert len(tree) == 584
    assert tree[0] == (
        "R115228242",
        "C0694449",
        "C0178238",
        "HT",
        7,
        "A00-A09",
        "Intestinal infectious diseases (A00-A09)",
        3,
    )
    assert tree[-1] == (
        "R210408482",
        "C0700345",
        "C5674868",
        "PT",
        6,
        "B37.31",
        "Acute candidiasis of vulva and vagina",
        6,
    )
