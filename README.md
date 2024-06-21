# Cumulus Library UMLS

An installation of the Unified Medical Language System® Metathesaurus®. Part of the [SMART on FHIR Cumulus Project](https://smarthealthit.org/cumulus)

For more information, [browse the documentation](https://docs.smarthealthit.org/cumulus/library).
## Usage

In order to use the Metathesaurus, you'll need to get an API key for access from the National Library of Medicine, which you can sign up for [here](https://uts.nlm.nih.gov/uts/signup-login).

You can then install this module by running `pip install cumulus-library-umls`.

This will add a `umls` target to `cumulus-library`. You'll need to pass your
API key via the `--umls-key` CLI flag, or set the `UMLS_API_KEY` environment variable
to the key you received from NIH.

This ends up being a fairly intensive operation - we download a large file,
extract it, create parquet files from Athena, and then upload it. It usually
takes a half hour to run. We try to preserve some of those artifacts along
the way to make rebuilds faster. If you need to force recreation from scratch, the
`--force-upload` CLI flag will handle this.

Note: This study is explicitly namespaced in its own schema, `umls`. Make sure your
database is not using this schema for another use. Do not create tables inside this
schema by another means.

## Licensing details

The `cumulus-library-umls` study is provided as a convenience to install the
UMLS Metathesaurus, but is not shipped with the Metathesaurus dataset. It will
require an API key to download the data from NIH directly.

As a reminder, the 
[License Agreement for Use of the UMLS® Metathesaurus®](https://uts.nlm.nih.gov/uts/assets/LicenseAgreement.pdf)
provides several restrictions on this usage of this data (including distributing
the dataset). When you sign up for a UMLS key, you are assuming responsibility
for complying with these terms, or an alternate licensing agreement with the
owner of the Metathesaus data if you are provided with one.


## Citations

Bodenreider O. The Unified Medical Language System (UMLS): integrating biomedical terminology. Nucleic Acids Res. 2004 Jan 1;32(Database issue):D267-70. doi: 10.1093/nar/gkh061. PubMed PMID: 14681409; PubMed Central PMCID: PMC308795.