-- Create the database
CREATE database dedup;


--Schema for intraday-prices table
DROP TABLE IF EXISTS "file_properties";

CREATE TABLE "file_properties" (
 
filename   		TEXT        NOT NULL,
filesize      	BIGINT      NOT NULL,
directorypath   TEXT        NOT NULL,
hash        	TEXT        NULL,


PRIMARY KEY(directorypath, filename)
 
);