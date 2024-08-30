"""Builder for UMLS files that generally do not change over time"""
import dataclasses
import pathlib

import pandas
from cumulus_library import base_table_builder, base_utils, study_manifest
from cumulus_library.template_sql import base_templates


@dataclasses.dataclass(kw_only=True)
class StaticTableConfig:
    """Convenience class for holding params for configuring tables from flat files"""

    file_path: str
    delimiter: str
    table_name: str
    headers: list[str]
    dtypes: dict
    parquet_types: list[str]
    ignore_header: bool = False


class StaticBuilder(base_table_builder.BaseTableBuilder):
    base_path = pathlib.Path(__file__).resolve().parent
    display_text = "Building static UMLS tables..."

    def get_table_configs(self):
        return [
            # https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/documentation/SemanticTypesAndGroups.html
            StaticTableConfig(
                file_path=self.base_path / "./static_files/SemanticTypes_2018AB.txt",
                delimiter="|",
                table_name="semantic_types",
                headers=["abbrev", "TUI", "full_type_name"],
                dtypes={
                    "type_abbrev": "str",
                    "TUI": "str",
                    "full_type_name": "str",
                },
                parquet_types=["STRING", "STRING", "STRING"],
            ),
            StaticTableConfig(
                file_path=self.base_path / "./static_files/SemGroups_2018.txt",
                delimiter="|",
                table_name="semantic_groups",
                headers=["abbrev", "group_name", "TUI", "full_type_name"],
                dtypes={
                    "group_abbrev": "str",
                    "group_name": "str",
                    "TUI": "str",
                    "full_type_name": "str",
                },
                parquet_types=["STRING", "STRING", "STRING", "STRING"],
            ),
            # https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html
            StaticTableConfig(
                file_path=self.base_path / "./static_files/umls_rel.tsv",
                delimiter="\t",
                table_name="rel_description",
                headers=["REL", "REL_STR"],
                dtypes={
                    "REL": "str",
                    "REL_STR": "str",
                },
                parquet_types=["STRING", "STRING"],
                ignore_header=True,
            ),
            StaticTableConfig(
                file_path=self.base_path / "./static_files/umls_rela.tsv",
                delimiter="\t",
                table_name="rela_description",
                headers=["RELA", "RELA_STR"],
                dtypes={
                    "RELA": "str",
                    "RELA_STR": "str",
                },
                parquet_types=["STRING", "STRING"],
                ignore_header=True,
            ),
            StaticTableConfig(
                file_path=self.base_path / "./static_files/umls_tty.tsv",
                delimiter="\t",
                table_name="tty_description",
                headers=["TTY", "TTY_STR"],
                dtypes={
                    "TTY": "str",
                    "TTY_STR": "str",
                },
                parquet_types=["STRING", "STRING"],
                ignore_header=True,
            ),
            StaticTableConfig(
                file_path=self.base_path / "./static_files/umls_tui.tsv",
                delimiter="\t",
                table_name="tui_description",
                headers=["STY", "TUI", "TUI_STR"],
                dtypes={
                    "STY": "str",
                    "TUI": "str",
                    "TUI_STR": "str",
                },
                parquet_types=["STRING", "STRING", "STRING"],
                ignore_header=True,
            ),
        ]

    def prepare_queries(
        self,
        config: base_utils.StudyConfig,
        manifest: study_manifest.StudyManifest,
        *args,
        **kwargs,
    ):
        # fetch and add vsac tables
        self.tables = self.get_table_configs()
        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(
                "Uploading UMLS dictionary files...", total=len(self.tables)
            )

            for table in self.tables:
                # Determine what we're using as a source file
                path = self.base_path / table.file_path
                parquet_path = (
                    table.file_path.parent
                    / table.file_path.stem
                    / f"{table.file_path.stem}.parquet"
                )
                parquet_path.parent.mkdir(parents=True, exist_ok=True)
                # Read the file, using lots of the TableConfig params, and generate
                # a parquet file
                df = pandas.read_csv(
                    path,
                    delimiter=table.delimiter,
                    names=table.headers,
                    header=0 if table.ignore_header else None,
                    dtype=table.dtypes,
                    index_col=False,
                    na_values=["\\N"],
                )
                df.to_parquet(parquet_path)
                # Upload to S3 and create a table that reads from it
                prefix = manifest.get_study_prefix()
                remote_path = config.db.upload_file(
                    file=parquet_path,
                    study=prefix,
                    topic=parquet_path.stem,
                    force_upload=config.force_upload,
                )
                self.queries.append(
                    base_templates.get_ctas_from_parquet_query(
                        schema_name=config.schema,
                        table_name=f"{prefix}__{table.table_name}",
                        local_location=parquet_path.parent,
                        remote_location=remote_path,
                        table_cols=table.headers,
                        remote_table_cols_types=table.parquet_types,
                    )
                )
                progress.advance(task)
