
The data in this directory is extracted from the Access97 table defintion file 
at:  http://www.census.gov/support/2000/SF2/Access97.zip

mdb-export SF2.MDB TABLES > meta/2000-sf2-columns.csv


Many of the data columns which should be integers are instead  varchars because the columns contain hash marks. The Has marks ('#') will be in a string that is as long as the field is wide. These columns are marked in the schema with the 'has_hash' metadata value. ('d_has_hash' column in the schema file)
