import pathlib

import pandas
from cumulus_library import base_table_builder, base_utils, databases
from cumulus_library.apis import umls
from cumulus_library.template_sql import base_templates


class UMLSBuilder(base_table_builder.BaseTableBuilder):
    def rmtree(self, root: pathlib.Path):
        """Deletes a dir and all files underneath

        :param root: the location at the base of the path you want to remove

        TODO: replace with native pathlib.walk when upgrading to python 3.12
        """

        # just in case, if we get passed a file (like if there's an error and a
        # zip file exists in the download dir)
        if not root.is_dir():
            root.unlink()
        else:
            for p in root.iterdir():
                if p.is_dir():
                    self.rmtree(p)
                else:
                    p.unlink()
            root.rmdir()

    def get_umls_data(
        self,
        download_path: pathlib.Path,
        parquet_path: pathlib.Path,
        force_upload: bool,
        umls_key: str,
    ) -> (list, bool, str):
        """Fetches and extracts data from the UMLS API

        :param download_path: the location to read from
        :param parquet_path: the location output is written; only used for deletion
            if a new dataset is downloaded
        :param force_upload: if True, will download from UMLS regardless of data on disk
        :param umls_key: the UMLS API key to use to auth requests
        :returns:
            - filtered_files - a list of files to process (excluding language tables)
            - download_required - if True, a new UMLS release needed to be retrieved
            - release_version - the name of the folder data was extracted to
        """
        api = umls.UmlsApi(api_key=umls_key)
        metadata = api.get_latest_umls_file_release(
            target="umls-metathesaurus-full-subset"
        )
        download_required = False
        if not (download_path / metadata["releaseVersion"]).exists():
            print("New UMLS release available, downloading & updating...")
            download_required = True
            for version in download_path.iterdir():
                self.rmtree(version)
            for version in (parquet_path).iterdir():
                self.rmtree(version)
        if download_required or force_upload:
            api.download_umls_files(
                target="umls-metathesaurus-full-subset", path=download_path
            )
        files = list(download_path.glob(f'./{metadata["releaseVersion"]}/META/*.ctl'))
        filtered_files = []
        for file in files:
            if not file.stem.startswith("MRX"):
                filtered_files.append(file)
        return filtered_files, download_required, metadata["releaseVersion"]

    def sql_type_to_df_parquet_type(self, text: str) -> str:
        """Converts types extract from the MySQL .ctl definition to parquet types

        :param text: the type to convert
        :returns: the parquet type
        """
        text = text.split("(")[0].strip(",").replace(" external", "")
        match text:
            case "char":
                return "string", "String"
            case "integer":
                return "Int64", "Integer"
            case "float":
                return "float", "Float"
            case _:
                raise Exception(f"'{text}' missing a type converter")

    def parse_ctl_file(self, contents: list[str]) -> (str, dict):
        """Extracts table and type definitions from a *.ctl file

        :param contents: an array of strings, expected from a file.readlines call()
        :returns:
            - datasource - the name of the datasource for population
            - table -a dict describing the table
        """
        datasource = None
        table = {"headers": [], "dtype": {}, "parquet_types": []}
        is_col_def_section = False
        for line in contents:
            if line is None:
                continue
            if line.startswith("infile"):
                datasource = line.split(" ")[1].rstrip().replace("'", "")
            elif line.startswith("("):
                is_col_def_section = True
                line = line[1:]
            elif line.startswith(")"):
                is_col_def_section = False
            if is_col_def_section:
                if line is not None:
                    line = line.strip().split("\t")
                    df_type, parquet_type = self.sql_type_to_df_parquet_type(line[1])
                    table["headers"].append(line[0])
                    table["dtype"][line[0]] = df_type
                    table["parquet_types"].append(parquet_type)
        return datasource, table

    def create_parquet(
        self,
        rrf_path: pathlib.Path,
        parquet_path: pathlib.Path,
        table: dict[list],
        force_upload=False,
    ):
        """Creates a parquet file from a .rrf metathesaurus file

        :param rrf_path: the location of the .rrf files
        :param parquet_path: the location to write output parquet to
        :param table: a table definition created by parse_ctl_files
        :param force_upload: if true, upload to a remote source regardless of what
            already exists there
        """
        if not force_upload:
            if (parquet_path / f"{rrf_path.stem}.parquet").exists():
                return
        df = pandas.read_csv(
            rrf_path,
            delimiter="|",
            names=table["headers"],
            dtype=table["dtype"],
            index_col=False,
        )
        df.to_parquet(parquet_path / f"{rrf_path.stem}.parquet")

    def prepare_queries(
        self,
        cursor: databases.DatabaseCursor,
        schema: str,
        *args,
        config=base_utils.StudyConfig,
        **kwargs,
    ):
        download_path = pathlib.Path(__file__).resolve().parent / "downloads"
        download_path.mkdir(exist_ok=True, parents=True)
        parquet_path = pathlib.Path(__file__).resolve().parent / "generated_parquet"
        parquet_path.mkdir(exist_ok=True, parents=True)
        files, new_version, folder = self.get_umls_data(
            download_path, parquet_path, config.force_upload, config.umls_key
        )
        parquet_path = parquet_path / folder
        parquet_path.mkdir(exist_ok=True, parents=True)

        with base_utils.get_progress_bar() as progress:
            task = progress.add_task(
                None,
                total=len(files),
            )
            for file in files:
                with open(file) as f:
                    datasource, table = self.parse_ctl_file(f.readlines())
                    progress.update(task, description=f"Compressing {datasource}...")
                    rrf_path = download_path / f"./{folder}/META/{datasource}"
                    self.create_parquet(
                        rrf_path, parquet_path, table, force_upload=config.force_upload
                    )
                    progress.update(task, description=f"Uploading {datasource}...")
                    remote_path = config.db.upload_file(
                        file=parquet_path / f"{file.stem}.parquet",
                        study="umls",
                        topic=file.stem,
                        remote_filename=f"{file.stem}.parquet",
                        force_upload=config.force_upload or new_version,
                    )
                    self.queries.append(
                        base_templates.get_ctas_from_parquet_query(
                            schema_name=schema,
                            table_name=f"umls__{file.stem}",
                            local_location=parquet_path / f"{file.stem}.parquet",
                            remote_location=remote_path,
                            table_cols=table["headers"],
                            remote_table_cols_types=table["parquet_types"],
                        )
                    )
                    progress.advance(task)
