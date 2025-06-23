import pathlib

from cumulus_library import base_utils, databases, db_config

from cumulus_library_umls import umls_builder


def test_ancillary_tables(tmp_path):
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
        filepath = pathlib.Path(__file__).parent.parent / "tests/test_data/2000AA/META/"
        with open(filepath / f"{file}.ctl") as f:
            datasource, table = builder.parse_ctl_file(f.readlines())
            rrf_path = filepath / f"{file}.RRF"
            builder.create_parquet(rrf_path, tmp_path, table)
            cursor.execute(
                f"""CREATE TABLE "umls__{file}" AS SELECT "{'","'.join(columns)}"
            FROM read_parquet('{tmp_path}/{file}/{file}.parquet')"""
            )

    with open(
        pathlib.Path(__file__).parent.parent / "cumulus_library_umls/ancillary_tables.sql"
    ) as f:
        queries = f.read().split(";")
        for query in queries:
            cursor.execute(query)
    tree = cursor.execute("SELECT  * FROM umls__icd10_tree ORDER BY rui ASC").fetchall()
    assert len(tree) == 584
    print(tree[0])
    assert tree[0] == (
        "R115228242",
        "C0694449",
        "C0178238",
        "HT",
        "A00-A09",
        "Intestinal infectious diseases (A00-A09)",
        3,
    )
    assert tree[-1] == (
        "R210408482",
        "C0700345",
        "C5674868",
        "PT",
        "B37.31",
        "Acute candidiasis of vulva and vagina",
        5,
    )

    hierarchy = cursor.execute(
        "SELECT  * FROM umls__icd10_hierarchy ORDER BY subcategory_2_code ASC"
    ).fetchall()
    assert len(hierarchy) == 934
    assert hierarchy[0] == (
        "A00-B99",
        "Certain infectious and parasitic diseases (A00-B99)",
        "A50-A64",
        "Infections with a predominantly sexual mode of transmission (A50-A64)",
        "A50.9",
        "Congenital syphilis, unspecified",
        "A50.0",
        "Early congenital syphilis, symptomatic",
        "A50.01",
        "Early congenital syphilitic oculopathy",
        None,
        None,
        None,
        None,
    )
    assert hierarchy[-1] == (
        "A00-B99",
        "Certain infectious and parasitic diseases (A00-B99)",
        "B99-B99",
        "Other infectious diseases (B99)",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
