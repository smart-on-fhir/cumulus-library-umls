from cumulus_library import base_utils, databases, db_config, study_manifest

from cumulus_library_umls import static_builder


def test_static_tables(tmp_path):
    db_config.db_type = "duckdb"
    config = base_utils.StudyConfig(
        db=databases.DuckDatabaseBackend(f"{tmp_path}/duckdb"),
        umls_key="123",
        schema="main",
    )
    cursor = config.db.cursor()
    cursor.execute("CREATE SCHEMA umls")
    manifest = study_manifest.StudyManifest()
    manifest._study_config = {
        "study_prefix": "umls",
        "advanced_options": {"dedicated_schema": "umls"},
    }
    manifest._advanced_config = {}
    builder = static_builder.StaticBuilder()
    builder.execute_queries(config=config, manifest=manifest)

    for table_conf in [
        {
            "name": "semantic_types",
            "size": 127,
            "first": ("aapp", "T116", "Amino Acid, Peptide, or Protein"),
            "last": ("vtbt", "T010", "Vertebrate"),
        },
        {
            "name": "semantic_groups",
            "size": 127,
            "first": ("ACTI", "Activities & Behaviors", "T052", "Activity"),
            "last": (
                "PROC",
                "Procedures",
                "T061",
                "Therapeutic or Preventive Procedure",
            ),
        },
        {
            "name": "rel_description",
            "size": 13,
            "first": ("AQ", "Allowed qualifier"),
            "last": ("XR", "Not related, no mapping"),
        },
        {
            "name": "rela_description",
            "size": 1023,
            "first": (
                "abnormal_cell_affected_by_chemical_or_drug",
                "abnormal cell affected by chemical or drug",
            ),
            "last": ("wound_has_communication_with", "Wound has communication with"),
        },
        {
            "name": "tty_description",
            "size": 242,
            "first": ("AA", "Attribute type abbreviation"),
            "last": ("XQ", "Alternate name for a qualifier"),
        },
        {
            "name": "tui_description",
            "size": 127,
            "first": ("aapp", "T116", "Amino Acid, Peptide, or Protein"),
            "last": ("vtbt", "T010", "Vertebrate"),
        },
    ]:
        res = cursor.execute(f"SELECT * from umls.{table_conf['name']}").fetchall()
        assert len(res) == table_conf["size"]
        assert res[0] == table_conf["first"]
        assert res[-1] == table_conf["last"]
