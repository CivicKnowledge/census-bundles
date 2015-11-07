

# Caveats

Jam Values. For many values there are four special "jam values" that have special meanings. Unfortunalte, there is no indication in the ACS documentation which tables or values have these values. The values are listed in the [Technical Documentation] (http://www2.census.gov/acs2009_5yr/summaryfile/ACS_2005-2009_SF_Tech_Doc.pdf), page 32. They are: 

- '.' A missing value, because no estimate was available or it was suppressed
- ' ' Geographic restriction. These values are missing because they were not calculated. 

For margins of error:

- 0. The Margin of error of 0 means the estimate is controlled, so statistical tests are not warranted. 
- -1. The estimate does not have a margin of error, which occurs on a few tables. 

In this conversion, all of these values are converted to NULL and a code is added to the ``jam_values`` field to indicate the original jam value. Every NULL in the table will have a in the ``jam_values`` field, so if a table has 3 NULLs, there will be three characters in the ``jam_values`` field. The conversions from jam values to codes in the ``jam_values`` field is:

- '.' -> 'm'
- ' ' -> 'g'
- NULL -> 'N'

Because the jam_values value can be really long -- 60 'm' characters for an empty row -- it is run length encoded.

The MOE values ( 0 and -1 ) are unaltered

