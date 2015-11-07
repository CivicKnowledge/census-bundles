
# Data Notes

For the SDLU ( Lower State Legislative District) the value ZZZ indicates there is no lower district. These are mapped to NULLs

# Wrangler Notes

Unlike most bundles, the ``dest_table`` specified in ``sources.csv`` is not the table that the gefile data will be written to. All of the data was written to the ``geofile`` table for the first run, then the ``meta_build_reduced_schemas()`` method was run to create the individual tables for each of the summary levels. 

When building, the table that is used in each partition is set by the ``SelectPartition`` in the ``pipelines`` configuration. 
