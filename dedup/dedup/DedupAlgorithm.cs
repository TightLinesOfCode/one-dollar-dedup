using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// .NET libraries
using System.IO;
using System.Security.Cryptography;

// 3rd Party Libraries
using Serilog;
using Npgsql;


namespace dedup
{
    public static class DedupAlgorithm
    {

        public static void ProcessDedup()
        {
            // Connect to the database, select all files that do not have a hash, sort by largest size, then shortest path, and select the first record

            try
            {
              

                // Sets up database connection
                var cs = "Host=localhost;Username=" + Constants.SQLDatabaseUserName + ";Password=" + Constants.SQLDatabasePassword + ";Database=" + Constants.SQLDatabaseName + ";";
                // Setup connection for the query
                using var con = new NpgsqlConnection(cs);
                con.Open();
                // Get Columns and create data table 
                string sql = "Select filename, filesize, directorypath FROM file_properties ORDER by filesize ASC, filename, directorypath ASC;";
                // Execute query
                using var cmd = new NpgsqlCommand(sql, con);
                using NpgsqlDataReader rdr = cmd.ExecuteReader();


                string fileName = "";
                long filesize = 0;
                string directoryPath = "";
                string hash = null;

                // Throws error if the database has no rows
                if (!rdr.HasRows)
                {
                    throw new System.ArgumentException("Failure on file_properties database");
                }
                while (rdr.Read())
                {
                    // Iterate through each row and look for duplicates
                    string dupFileName = rdr.GetString(0);
                    long dupFileSize = rdr.GetInt64(1);
                    string dupDirectoryPath = rdr.GetString(2);
                    string duphash = null;

                                       
                    // Check for duplicate file names and duplicate file lengths
                    if((dupFileName == fileName)&&(dupFileSize == filesize))
                    {
                        if (File.Exists(dupDirectoryPath + "\\" + dupFileName))
                        {
                            // Process duplicate file
                            duphash = CreateMD5(dupDirectoryPath + "\\" + dupFileName);
                        }
                        else
                        {
                            // Remove the item from the database
                            removeDatabaseRecord(dupFileName, dupFileSize, dupDirectoryPath, duphash);
                        }
                        // Check to see if hash files match if so, then move the dupfile
                        if(duphash == hash)
                        {
                            // Move the dupfile
                            try
                            {
                                // Set paths
                                string dupPathSource = dupDirectoryPath + "\\";
                                string dupPathDestination = @"\\?\UNC\169.254.7.147\TempFiles\Duplicates\" + dupDirectoryPath.Remove(0, 22) + "\\";

                                // Check if directory exists, if not, then create it
                                bool exists = System.IO.Directory.Exists(dupPathDestination);
                                if (!exists)
                                {
                                    System.IO.Directory.CreateDirectory(dupPathDestination);
                                }

                                if(filesize > 0)
                                {
                                    // Do Nothing, but use for debugging
                                }

                                // Move the file
                                if (File.Exists(dupPathSource + dupFileName))
                                {
                                    File.Move(dupPathSource + dupFileName, dupPathDestination + dupFileName);
                                }
                            }
                            catch (Exception ex)
                            {
                                Log.Error(ex.ToString());
                            }
                        }
                    }
                    else
                    {
                        // Assume this is a new file and update the master
                        fileName = dupFileName;
                        filesize = dupFileSize;
                        directoryPath = dupDirectoryPath;
                        hash = CreateMD5(dupDirectoryPath + "\\" + dupFileName);
                    }
                }
            }

            catch (Exception ex)
            {
                //Log.Information("{0} problem with processing : {1}", ex.ToString(), filePath);
                Log.Warning("Problem with database dedup query {0}", ex);

            }


            

            

            

            

            



        }

        public static string CreateMD5(string filename)
        {
            using (var md5 = MD5.Create())
            {
                using (var stream = File.OpenRead(filename))
                {
                    // Generate hash value(Byte Array) for input data
                    var hashBytes = md5.ComputeHash(stream);

                    // Convert hash byte array to string
                    var hash = BitConverter.ToString(hashBytes).Replace("-", string.Empty);

                    //return Encoding.Default.GetString(md5.ComputeHash(stream));
                    return hash;
                }
            }
        }

        public static  void removeDatabaseRecord(string filename, long filesize, string directorypath, string hash)
        {

            try
            {
                // Sets up database connection
                var cs = "Host=localhost;Username=" + Constants.SQLDatabaseUserName + ";Password=" + Constants.SQLDatabasePassword + ";Database=" + Constants.SQLDatabaseName + ";";
                using var con = new NpgsqlConnection(cs);
                con.Open();

                string tempsql = "DELETE FROM file_properties " +
                                                    "WHERE filename = '" + filename + "' " +
                                                    "AND filesize = '" + filesize + "' " +
                                                    "AND directorypath = '" + directorypath + "' " +  
                                                    "AND hash = '" + hash + "'" +
                                                    ";";

                var cmd = new NpgsqlCommand(tempsql, con);

                Log.Debug(tempsql);

                cmd.ExecuteNonQuery();

                Log.Debug("Removed {0} : {1} bytes : from database @ {2}", filename, filesize, directorypath);
            }
            catch (Exception ex)
            {
                //Log.Information("{0} problem with processing : {1}", ex.ToString(), filePath);
                Log.Warning("Problem with database delete {0}", ex);

            }


        }

        public static void DeleteEmptyDirs(string dir)
        {
            if (String.IsNullOrEmpty(dir))
                throw new ArgumentException(
                    "Starting directory is a null reference or an empty string",
                    "dir");

            try
            {
                foreach (var d in Directory.EnumerateDirectories(dir))
                {
                    DeleteEmptyDirs(d);
                }

                var entries = Directory.EnumerateFileSystemEntries(dir);

                if (!entries.Any())
                {
                    try
                    {
                        //Directory.Delete(dir);
                        Directory.Move(dir, @"\\?\UNC\169.254.7.147\TempFiles\EmptyDirectories\" + dir.Remove(0, 22) + "\\");
                    }
                    catch (UnauthorizedAccessException) { }
                    catch (DirectoryNotFoundException) { }
                }
            }
            catch (UnauthorizedAccessException) { }
        }


    }
}




/*
 * 
 * 
 * 
 * // Check the database to see if there are any duplicates for this file (filesize, similar file name, then hash the second file and compare hashes )
                    // If the hash matches, then move the file to the deleted folder using same path structure, 
                    // Verify the moved hash a second time, if problem, then print file name and stop the program by getting character input
                    // If verification of second hash works, then duplicate records from the database


                        // Then continue search of database for duplicates ... i.e. same file size and similar name -- need to figure out some percentage of duplication on the names ...



 * 
 * // Check the hash of this first row and see if it's empty and if it is empty, then read file and create hash and upsert
                    // If this first record and put into the database hash field
                    if (hash != null)
                    {
                        // Sets current state and looks for next duplicate
                        fileName = rdr.GetString(0);
                        fileLength = rdr.GetInt64(1);
                        directoryPath = rdr.GetString(2);
                        hash = rdr.GetString(3);
                    }
                    else if (hash == null)
                    {
                        // Check against the master and if it is a duplicate than process the duplicate file


                        // Create the hash
                        hash = CreateMD5(directoryPath + "\\" + fileName);

                        // Compare against the master

                        TODO: If it's a duplicate, then move the file, then delete the item in the database

                        // If it's new then update the hash field of the record in the database

                        TODO:

                        // And since its new, then set the master and continue to next record
*/
