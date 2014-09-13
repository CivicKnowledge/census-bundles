
# 2008 to 2012 American Community Survey

*Warning to novice users:* Unlike the decennial census, which counts every person in the country, the ACS is based on statistical sampling, with a lot processing. The data can be very difficult to use correctly. You should be comforatable with sampling, variance, estimation and margins of error. The Missouri Census Data Center has a [good overview of complexities with the 2005 ACS ](http://mcdc.missouri.edu/data/acs2005/Ten_things_to_know.shtml) that serves as a good introduction to many of the complexities of the ACS. Additionally, read the external documentation linked this bundle for the technical details of the release, as well as overviews of the variance estimation and weighting procedures. 



## Notes

Partitions are organized around states and tables.

## Changes to source

There are a few tables, like B24121 through B24126, that are spread across multiple segments. B24121 has about 530 columns, but the CSV files are limited to 252 columns or so. Relational databases have a hard time with a large number of columns. 

For these tables, the table is named with the Sequence Number appended, so table B24121 is spread across multiple tables that have tables names such as `b24121_85` through `b24121_86`

### Jam Codes

Jam codes are incompletely processed. Jam codes that can't be cast to an integer, such as '.', are replaced with a NULL. A future version will include a '_code' column i the tables that have Jam Codes. These new columns will hold the original value, while the data column will hold a NULL. 

There are two integer jam codes: '0' and '-1'. These values are still in the measures, so users must ensure that their queries avoid these rows when computing statistics. 
