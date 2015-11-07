
The column_meta and table_meta tables are from Censusreporter, https://github.com/censusreporter/census-table-metadata

THe list of summary levels is from the Missouria Census Data Center, http://mcdc2.missouri.edu/cgi-bin/browse?/pub/sasfmats/Ssumlev.sas@

# Development Notes

THe bulid-table_shells removes the 'year' column because in the original source
it is a string, while the field value added in building is an int. The difference causes a caster error. 

The 2009 3 year table sequence source ( 2009-3-sequence ) and possibly others, 
has an odd case where there are line number out of sequence. For instance, the first column of the B07402 has a line number of 5, rather than the typical 1. There are many such lines, most of the time they are accompanied with a table title that ends in '--', but there are others, like (19081) where they don't 

Census Reporter deals with [this in their code](https://github.com/censusreporter/census-table-metadata/blob/master/process_merge.py#L373) by looking for titles that end in '--', noting that the line numbers in these lines are supposed to have decimals in them, but in the 2009 3 year, they do not -- the line numbers are all to large by a factor of 10. 
