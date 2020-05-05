# Open Targets Post-Pipeline ETL process

OpenTargets ETL pipeline to process Pipeline output in order to obtain a new API shaped entities. For the file 
`platformDataBackend.sc`

## platformDataBackend.sc

### Requirements

1. OpenJDK 1.8
2. scala 2.12.x (through SDKMAN is simple)
3. ammonite REPL
4. Drug index dump from OpenTargets ES
5. Target index dump from OpenTargets ES
6. Disease index dump from OpenTargets ES
7. Evidence index dump from OpenTargets ES
8. Expression index dump from OpenTargets ES


### Run platformETL.sc
```sh
# to compute the dataset
In order to run the script the user must provide a config file called "amm.application.conf"
An example is avaible under
resources/amm.application.conf.example

export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G> \
    -Dconfig.file=resources/application.conf \
    -Dlogback.configurationFile=resources/logback.xml"

The command below will build the disease, target and drug indexes using the config file under resources/amm.application.conf

time amm platformETL.sc  

The parameters conf and step (disease,target,drug) can be use to customise the index to build
Eg.
time amm platformETL.sc
time amm platformETL.sc -step disease
time amm platformETL.sc -step target

```


### Run the scala script: platformDataBackend.sc

```sh
export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G>"
# to compute the dataset
time amm platformDataBackend.sc \
    --drugFilename 19.06_drug-data.json \
    --targetFilename 19.06_gene-data.json \
    --diseaseFilename 19.06_efo-data.json \
    --evidenceFilename 19.06_evidence-data.json \
    --outputPathPrefix out/
```

### Generate the indices dump from ES7

You will need to either connect to a machine containing the ES or forward the ssh port from it
```sh
elasticdump --input=http://localhost:9200/<indexyouneed> \
    --output=<indexyouneed>.json \
    --type=data  \
    --limit 10000 \
    --sourceOnly
```

## platformEvidenceDrugAggregation.sc

This script generates the Disease-drug-phase-status-target... aggregation is order to obtain a ready 
to aggregate table for drugs and clinical trials. To run it just use `--help` to get the list of 
command line parameters.



### Create a fat JAR
Simply run the following command:

```bash
sbt assembly
```
The jar will be generated under target/scala2.12.10/

### Scalafmt Installation

A pre-commit hook to run [scalafmt](https://scalameta.org/scalafmt/) is recommended for 
this repo though installation of scalafmt is left to developers. The [Installation Guide](https://scalameta.org/scalafmt/docs/installation.html)
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
