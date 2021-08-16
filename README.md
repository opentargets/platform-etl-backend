[![Build Status](https://travis-ci.com/opentargets/platform-etl-backend.svg?branch=master)](https://travis-ci.com/opentargets/platform-etl-backend)

# Open Targets Post-Pipeline ETL process

OpenTargets ETL pipeline to process Pipeline output in order to obtain a new API shaped entities. For the file 
`platformDataBackend.sc`

## platformDataBackend.sc

### Requirements

1. OpenJDK 8/11
2. scala 2.12.x (through SDKMAN is simple)
3. ammonite REPL
4. Drug index dump from OpenTargets ES
5. Target index dump from OpenTargets ES
6. Disease index dump from OpenTargets ES
7. Evidence index dump from OpenTargets ES
8. Expression index dump from OpenTargets ES
9. Generate MousePhenotypes dump from OperTargets ES

#### Notes on Java version

Either Java 8 or 11 can be used to build and run the project, but if you intend to use the compiled jar file on
_Dataproc_ you must use Java 8. To avoid this problem altogether, do not use native Java methods unless strictly
necessary.

### Generate the indices dump from ES7

You will need to either connect to a machine containing the ES or forward the ssh port from it

```sh
elasticdump --input=http://localhost:9200/<indexyouneed> \
    --output=<indexyouneed>.json \
    --type=data  \
    --limit 10000 \
    --sourceOnly
```

### Mouse Phenotype input for ETL (deprecated as of 21.06 release!)

Generate MousePhenotypes input file

```sh
cat 20.04_gene-data.json | jq -r '{"id":.id,"phenotypes": [.mouse_phenotypes[]?] }|@json' > mousephenotype.json
```

Copy the file in google storage or specific path

### Load with custom configuration

Add to your run either commandline or sbt task Intellij IDEA `-Dconfig.file=application.conf` and it will load the
configuration from your `./` path or project root. Missing fields will be resolved with `reference.conf`.

The most common configuration changes you will need to make are pointing towards the correct input files. To load files
we use a structure:

```
config-field-name {
      format = "csv"
      path = "path to file"
      options = [
        {k: "sep", v: "\\t"}
        {k: "header", v: true}
      ]
    }
```

The `options` field configures how Spark will read the input files. Both Json and CSV files have a large number of
configurable options, details of which can be
found [in the documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/DataFrameReader.html)

#### Configuring Spark

If you want to use a local installation of Spark customise the `application.conf` with the following spark-uri field and
adjust any other fields as necessary from the `reference.conf` template:

```conf
spark-uri = "local[*]"
common {
 ...  
}
```

By default Spark will only write to non-existent directories. This behaviour can be modified using the settings field
`spark-settings.write-mode` using one of the valid inputs "error", "errorifexists", "append", "overwrite", "ignore".

#### Configuring Loggging

Similarly update the logback configuration. You can add `-Dlogback.configurationFile=application.xml` and have a
logback.xml hanging on your project root or run path. An example log configuration file:

```xml

<configuration>

    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%level %logger{15} - %message%n%xException{10}</pattern>
        </encoder>
    </appender>

    <root level="WARN">
        <appender-ref ref="STDOUT" />
    </root>

    <logger name="io.opentargets.etl" level="DEBUG"/>
    <logger name="org.apache.spark" level="WARN"/>

</configuration>
```

and try to run one command as follows

```bash
java -server -Xms1G -Xmx6G -Xss1M -XX:+CMSClassUnloadingEnabled \
    -Dlogback.configurationFile=application.xml \
    -Dconfig.file=./application.conf \
    -classpath . \
    -jar io-opentargets-etl-backend-assembly-0.1.0.jar [step1 [step2 [...]]]
```

### Create a fat JAR

Simply run the following command:

```bash
sbt assembly
```
The jar will be generated under target/scala-2.12.10/

### Create cluster and launch

Here how to create a cluster using `gcloud` tool.


The current image version is `preview` because is the only image that supports `Spark3` and Java11.

List of [dataproc releases]([https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-preview])


```sh
gcloud beta dataproc clusters create \
    etl-cluster \
    --image-version=2.0-debian10 \
    --properties=yarn:yarn.nodemanager.vmem-check-enabled=false,spark:spark.debug.maxToStringFields=1024,spark:spark.master=yarn \
    --master-machine-type=n1-highmem-16 \
    --master-boot-disk-size=500 \
    --num-secondary-workers=0 \
    --worker-machine-type=n1-standard-16 \
    --num-workers=2 \
    --worker-boot-disk-size=500 \
    --zone=europe-west1-d \
    --project=open-targets-eu-dev \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --max-idle=30m
```

And to submit the job (the jar can also by specified from a gs://...

```sh
gcloud dataproc jobs submit spark \
           --cluster=etl-cluster \
           --project=open-targets-eu-dev \
           --region=europe-west1 \
           --async \
           --files=mk-latest.conf \
           --properties=spark.executor.extraJavaOptions=-Dconfig.file=mk-latest.conf,spark.driver.extraJavaOptions=-Dconfig.file=mk-latest.conf \
           --jar=gs://ot-snapshots/etl/jars/io-opentargets-etl-backend-assembly-0.2.5.jar -- disease
```

where `mk-latest.conf` is

```conf
common {
  output = "gs://ot-snapshots/etl/mk-latest"
}
```

### Adding ETL outputs to ElasticSearch

The `elasticsearch` directory in the project root folder includes utility scripts to load the outputs of the ETL into
a preconfigured ElasticSearch instance. 

#### Requirements

1. Python utility `elasticsearch_loader` must be installed and on your `PATH`
2. An open port of the ES instance must be forwarded to your local machine and execute the relevant script.

    ```
    gcloud beta compute ssh --zone "europe-west1-d" "es7-20-09" --project "open-targets-eu-dev" -- -L 9200:localhost:9200
    ```  

3. Update the `env.sh` script:
    - `PREFIX` refers to the path to the data to be loaded into Elasticsearch
    - `ES` is the url of Elasticsearch
    - `INDEX_SETTINGS` is the index configuration file. Typically this will be the `index_settings.json` file provided
      in the _elasticsearch_ directory.

4. Export the necessary environment variables by running `source [path to file]env.sh`
4. Run scripts relevant to the index you wish to create, or `load_all.sh` to load all of them.

## Step dependencies

As of June 2021 some steps of the ETL maintain dependencies on the old Data Pipeline which is being progressively
deprecated.

Considering only the inputs/outputs of the ETL there are component diagrams available in the 'documentation'
directory. _etl\_current_ shows the relationships between steps in the ETL. _etl\_current\_full_ shows those
relationships at a more granular level, where inputs and outputs are specifically specified.

_etl\_dp\_dependencies_ shows similar relationships, but often includes dependencies which result from the data
pipeline. This document will be removed once the deprecation of the data pipeline is complete.

The majority of the ETL was written to process data which has been prepared by the data pipeline for subsequent
processing. It is intended that this pipeline will be deprecated; because of this some steps do not require inputs from
the data pipeline to function correctly. These include:

- Drug
- Target

## Step notes

### Drug

The primary input source of the Drug dataset is ChEMBL. ChEMBL contains almost 2 million molecules, most which are are 
not 'drugs'. We define a drug to be any molecule that meets one or more of the following criteria: 
 - There is at least 1 known indication;
 - There is at least 1 known mechanism of action; or
 - The ChEMBL ID can be mapped to a DrugBank ID.  
 
To run the `Drug` step use the example command under `Create a fat JAR` with `drug` as the step name. 
 
#### Adding additional resources to enrich data

Addition resources can be specified to enrich the data included in the outputs. The following `extension-type`s are
 supported: 
 
 - `synonyms`
 - `cross-references`
 
See the sections below for more details on required data structure and limitations. 

Additional resources are specified in the configuration as follows:  
```
      drug-extensions = [
        {
          extension-type = <extension type>
          path = <path to file> 
        }
      ]
    }
```
##### Synonyms

The Drug Beta step supports the addition of supplementary synonym data sources subject to the following limitations:

- The input file(s) must be: 
  - in json format
  - have a field called 'id' which maps 1-to-1 to either a Drugbank ID or ChEMBL ID. The 'id' field must not contain a 
  mixture of both. If the ID is unknown the data will be discarded silently. If a mixture of ids are provided, it is
   indeterminate which of the two will be used. 
  - have a field called 'synonyms' which are either Strings or arrays of Strings linked to the 'id' field. 
  
The input files are specified in the configuration file under the field `drug-extensions`. The files can contain
 additional columns; these will be safely ignored. 
 
New synonyms are added to the 'synonyms' field on the object if they are not already present in either 'synonyms' or
'trade names'. At present it is not possible to add new fields to 'trade names'.

##### Cross references 

The Drug Beta step supports the addition of supplementary cross reference data sources subject to the following
 limitations:

- The input file(s) must: 
  - in json format
  - have a fields: 
    - 'id' which maps 1-to-1 to a ChEMBL ID.
    - 'source'
    - 'reference'  

For example: 

```jsonl
{"id": ..., "source": ..., "reference": ... }
```
  
The input files are specified in the configuration file under the field `drug-extensions`. The files can contain
 additional columns; these will be safely ignored. 
 
If the `source` already exists the new references will be appended to the existing ones, provided that the `reference
` is not already present. If the `source` does not exist it will be created. 
 
#### Inputs

Inputs are specified in the `reference.conf` file and include the following: 

| Name | Source |
| --- | --- |
|   `drug-chembl-molecule` | ChEMBL - Platform Input Support |
|   `drug-chembl-indication` | ChEMBL - Platform Input Support |
|   `drug-chembl-mechanism` | ChEMBL - Platform Input Support |
|   `drug-chembl-target` | ChEMBL - Platform Input Support |
|   `drug-drugbank` | Release annotation file |

The `Drug` step also relies on several other outputs from the ETL:

| Name in Drug | Field in configuration file |
| --- | --- |
| `efo` | disease | 
| `gene` | target | 
| `evidence` | evidence | 

#### Outputs

The `Drug` step writes three files under the common directory specified in the `drug.output.path` configuration field:

- drugs
- mechanismsOfAction
- Indications

Each of these outputs includes a field `id` to allow later linkages between them.

### GO

The 'Go' step generates a small lookup table of gene ontologies.

#### Inputs

| Input | Source | Notes |
| --- | --- | --- |
| go-input | PIS | Provided by PIS from http://geneontology.org/docs/download-ontology/#go_obo_and_owl |

The input is a flat file which does not lend itself to columnar processing so its currently a 'preprocessor' step. If
more complicated logic becomes required this should be ported. There is also to option of querying the EBI API but this
is quite slow and results in a moderately large dataset which we don't otherwise need.

### Mouse Phenotypes

#### Inputs

| Input | Source | Notes |
| --- | --- | --- |
| mp-classes | PIS | This is preprocessed by PIS using a project `opentargets-ontologyutils` to extract needed data from an OWL file in jsonl format. |
| mp-report | PIS | |
| mp-orthology | PIS | |
| mp-categories | Static | This file was a hard-coded map in the deprecated data-pipeline. |
| target | ETL | Output of target step of ETL | 

### Target

These notes refer to the Target step as rewritten in March 2021. If attempting to debug datasets completed before
release 20.XX consult commits preceeding XXXXXX.

#### Configuration

- `hgnc-orthology-species` lists the species to include in Target orthologues. The order of this configuration list is
  __significant__ as it is used to determine the order in which entries appear in the front-end. Items earlier in the
  list are more closely related to homo sapiens than items further down the list.

#### Inputs

Inputs to the ETL are prepared by Platform Input Support (PIS). PIS does some minimal preprocessing, but it is possible
to manually retrieve them and run the step locally. If you would like to run this step locally, retrieve the necessary
inputs from one of the Open Targets public input buckets. eg. `gs://ot-snapshots/...` rather than downloading the files
directly from the sources listed here which are included _for reference_.

Consult the `reference.conf` file to see how to configure the inputs, most of these require only changing the paths to
the data. Options for parsing the inputs should not need to be updated.

1. HGNC
   - `https://storage.googleapis.com/open-targets-data-releases/21.02/input/annotation-files/hgnc_complete_set-2021-02-09.json`

2. Ensembl

    - Use Ensembl human gene JSON file (available
      from: `ftp://ftp.ensembl. org/pub/release-102/json/homo_sapiens/homo_sapiens. json`) updating the release as
      required.
    - It can be useful to convert this file to jsonl format. It can be converted
      with `jq -c . genes[] homo_sapiens. json >> homo_sapiens.jsonl`. The file is 4GB, so needs a decent machine (min
      32GB RAM) for conversion.

3. Uniprot
    - the Uniprot format in flat txt format instead of xml.
    - This is a flat text file and is provided by PIS. Can be downloaded manually
      from `https://www.uniprot. org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=txt`
    - The is a conversion tool to create Scala objects in `io.opentargets.etl.preprocess.uniprot`
4. Gene Ontology
    - Requires files available from EBI:
        - [Annotation files for human proteins](ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz)
        - [Annotation files for human RNAs](ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human_rna.gaf.gz)
        - [File for eco lookup](ftp://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gpa.gz)
        - [RNAcentral to Ensembl mapping files](ftp://ftp.ebi.ac.uk/pub/databases/RNAcentral/current_release/id_mapping/database_mappings/ensembl.tsv)

5. Tep
    - Uses files downloaded for `tep` key in PIS's `config.yaml`.

4. NCBI
    - Used for synonyms, data available
      from: `ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens. gene_info.gz`
6. Human Protein Atlas

    - Used for subcellular locations. Data available
      from [HPA's website](https://www.proteinatlas.org/download/subcellular_location.tsv.zip)
7. Project Scores
8. ChEMBL
    - Target index
9. Gnomad
    - Used for genetic constraints. Data available
      from [Gnomad website](https://storage.googleapis.com/gcp-public-data--gnomad/release/2.1.1/constraint/gnomad.v2.1.1.lof_metrics.by_gene.txt.bgz)
    - The file is in `bgz` format, this can be converted to csv with `gunzip -c input > output.csv`.
10. Homologs
    - Update the release number as required:
        - ftp://ftp.ensembl.org/pub/release-100/tsv/ensembl-compara/homologies/homo_sapiens/Compara.100.protein_default.homologies.tsv.gz
        - ftp://ftp.ensembl.org/pub/release-100/tsv/ensembl-compara/homologies/homo_sapiens/Compara.100.ncrna_default.homologies.tsv.gz
        - ftp://ftp.ensembl.org/pub/release-100/species_EnsemblVertebrates.txt
    - Files generated by PIS: `104_homology_<species>.tsv` where '104' is the Ensembl Release. This is a file of name
      and gene ids to get the correct name for homology gene ids. There will be one for each species.
1. Reactome
    - Raw reactome file from [Reactome downloads](https://reactome.org/download-data) page, using
      the [Ensembl to pathways file](https://reactome.org/download/current/Ensembl2Reactome.txt)
    - Output of ETL step `reactome`

#### Homology species whitelist

This is used to select which species will be included in Target > Homologues. If you want to add a species to this list
you must also update Platform Input Support to retrieve that species' gene data.

## Development environment notes

### Scalafmt Installation

A pre-commit hook to run [scalafmt](https://scalameta.org/scalafmt/) is recommended for this repo though installation of
scalafmt is left to developers. The [Installation Guide](https://scalameta.org/scalafmt/docs/installation.html)
has simple instructions, and the process used for Ubuntu 18.04 was:

```bash
cd /tmp/  
curl -Lo coursier https://git.io/coursier-cli &&
    chmod +x coursier &&
    ./coursier --help
sudo ./coursier bootstrap org.scalameta:scalafmt-cli_2.12:2.2.1 \
  -r sonatype:snapshots \
  -o /usr/local/bin/scalafmt --standalone --main org.scalafmt.cli.Cli
scalafmt --version # "scalafmt 2.2.1" at TOW
```

The pre-commit hook can then be installed using:

```bash
cd $REPOS/platform-etl-backend
chmod +x hooks/pre-commit.scalafmt 
ln -s $PWD/hooks/pre-commit.scalafmt .git/hooks/pre-commit
```

After this, every commit will trigger scalafmt to run and ```--no-verify``` can be 
used to ignore that step if absolutely necessary.



# Copyright
Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
