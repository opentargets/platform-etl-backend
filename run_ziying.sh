export JAVA_OPTS="-Dlogback.configurationFile=logback.xml -server -Xms1G -Xmx20G -Xss1M -XX:+CMSClassUnloadingEnabled"
time amm scripts/opentargets-ziying.sc \
    --genesUri /home/mkarmona/src/opentargets/data/ziying/gene_parquet \
    --expressionUri /home/mkarmona/src/opentargets/data/ziying/expression_parquet \
    --l2gUri /home/mkarmona/src/opentargets/data/ziying/l2g \
    --studiesUri /home/mkarmona/src/opentargets/data/ziying/studies.parquet \
    --v2dUri /home/mkarmona/src/opentargets/data/ziying/v2d \
    --colocUri /home/mkarmona/src/opentargets/data/ziying/v2d_coloc \
    --outputUri ziying_out/
