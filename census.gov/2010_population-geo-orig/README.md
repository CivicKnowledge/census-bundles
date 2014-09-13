
# Caveats

This bundle is built by directly loading CSV, so it bypasses the caster. This 
means that the fields are exactly as they appear in the original source file, and sime cells can't be loaded into a database that does type checking. 

# Change Log

Revision 5. Added an index on stusab and geocomp, which is required in 
census.gov-2010_population-sf1-geo-429e-r1 to get, by state, the records that
are related to the layout of records in census.gov-geography-dim-orig-a7d9

Revision 6. Trim the name field. It was showing up as 90 characters long, padded with spaces, in installed bundles. 