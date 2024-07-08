# Cumulus Library UMLS

An installation of the Unified Medical Language System® RxNorm dataset. Part of the [SMART on FHIR Cumulus Project](https://smarthealthit.org/cumulus)

For more information, [browse the documentation](https://docs.smarthealthit.org/cumulus/library).
## Usage

In order to use RxNorm, you'll need to get an API key for access from the National Library of Medicine, which you can sign up for [here](https://uts.nlm.nih.gov/uts/signup-login).

You can then install this module by running `pip install cumulus-library-rxnorm`.

This will add an `rxnorm` target to `cumulus-library`. You'll need to pass your
API key via the `--umls-key` CLI flag, or set the `UMLS_API_KEY` environment variable
to the key you received from NIH.

We download a file from NIH, extract it, create parquet files for Athena, and then upload it.
We try to preserve some of those artifacts along the way to make rebuilds faster. If you need 
to force recreation from scratch, the `--force-upload` CLI flag will handle this.

Note: This study is explicitly namespaced in its own schema, `rxnorm`. Make sure your
database is not using this schema for another use. Do not create tables inside this
schema by another means.

## Licensing details

The `cumulus-library-rxnorm` study is provided as a convenience to install the
UMLS RxNorm dataset, but is not shipped with said dataset. It will
require an API key to download the data from NIH directly.

As a reminder, the 
[RxNorm terms of service](https://www.nlm.nih.gov/research/umls/rxnorm/docs/termsofservice.html)
provides several restrictions on this usage of this data (including distributing
the dataset). When you sign up for a UMLS key, you are assuming responsibility
for complying with these terms, or an alternate licensing agreement with the
owners of the RxNorm data if you are provided with one.


## NIH Requried Disclaimer

This product uses publicly available data courtesy of the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.

## Citations

Bodenreider O. The Unified Medical Language System (UMLS): integrating biomedical terminology. Nucleic Acids Res. 2004 Jan 1;32(Database issue):D267-70. doi: 10.1093/nar/gkh061. PubMed PMID: 14681409; PubMed Central PMCID: PMC308795.

Nelson SJ, Zeng K, Kilbourne J, Powell T, Moore R. Normalized names for clinical drugs: RxNorm at 6 years. J Am Med Inform Assoc. 2011 Jul-Aug;18(4)441-8. doi: 10.1136/amiajnl-2011-000116. Epub 2011 Apr 21. PubMed PMID: 21515544; PubMed Central PMCID: PMC3128404.