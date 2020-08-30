using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

// .NET libraries
using System.IO;
using System.Security.Cryptography;

// 3rd party libraries
using Npgsql;
// 3rd Party
using Serilog;
using Serilog.Sinks.SystemConsole;
using Serilog.Sinks.File;

namespace dedup
{
    // TODO:   Need to process the file folders and get rid of all "'"s in the directories.    Otherwise, there is not other way to recreate the file paths.


    class Program
    {
        

        static void Main(string[] args)
        {
            // Timer
            var watch = new System.Diagnostics.Stopwatch();
            watch.Start();

            // Setup Serilog logging
            string logFilePath = ".";
            Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            //.MinimumLevel.Debug()
            .WriteTo.Console()
            //.WriteTo.File(logFilePath, rollingInterval: RollingInterval.Day)
            .CreateLogger();

            // Start 
            Log.Information("Deduplication");

            //string directoryPath = @"C:\Users\John\Desktop";
            //string directoryPath = @"\\?\UNC\169.254.7.147\TempFiles\Backups";
            string directoryPath = @"\\?\UNC\169.254.7.147\TempFiles\Backups";

            // Main processing function
            //MapDirectory(new System.IO.DirectoryInfo(directoryPath));

            // Process the dedup operations on the database
            //DedupAlgorithm.ProcessDedup();
            DedupAlgorithm.MoveEmptyDirs(directoryPath);

            // Program execution time
            watch.Stop();
            Log.Information($"Execution Time: {watch.ElapsedMilliseconds} ms");

            // Finished running
            Log.Information("Deduplication Finished");
            Log.CloseAndFlush();

            // Exit
            Console.WriteLine("Hit enter to exit program");
            Console.ReadLine();
        }


        // Process all files in the directory passed in, recurse on any directories
        // that are found, and process the files they contain.
        public static void MapDirectory(System.IO.DirectoryInfo root)
        {
            System.IO.FileInfo[] files = null;
            System.IO.DirectoryInfo[] subDirs = null;

            // First, process all the files directly under this folder
            try
            {
                files = root.GetFiles("*.*");
            }
            // This is thrown if even one of the files requires permissions greater
            // than the application provides.
            catch (UnauthorizedAccessException e)
            {
                // This code just writes out the message and continues to recurse.
                // You may decide to do something different here. For example, you
                // can try to elevate your privileges and access the file again.
                //Log.Information(e.Message);
                Log.Warning("UnauthorizedAccessException {0}", root);
            }
            catch (System.IO.DirectoryNotFoundException e)
            {
                // This code just writes out the message and continues execution path
                //Log.Information(e.Message);
                Log.Warning("DirectoryNotFoundException {0}", root);
            }


            if (files != null)
            {
                foreach (System.IO.FileInfo fi in files)
                {
                    if(!ContainsInvalidChacterName(fi.Name))
                    {
                        // In this example, we only access the existing FileInfo object. If we
                        // want to open, delete or modify the file, then
                        // a try-catch block is required here to handle the case
                        // where the file has been deleted since the call to TraverseTree().
                        //Console.WriteLine(fi.FullName);
                        ProcessFile(fi.FullName);
                    }
                    else 
                    {
                        ProcessFile(FixFileName(fi).FullName);
                    }
                }

                // Now find all the subdirectories under this directory.
                subDirs = root.GetDirectories();

                foreach (System.IO.DirectoryInfo dirInfo in subDirs)
                {

                    // Resursive calls for each subdirectory.

                    // Check for apostrophe and rename directory then                     
                    if (!ContainsInvalidChacterName(dirInfo.ToString()))
                    {
                        MapDirectory(dirInfo);
                    }
                    else 
                    {
                        MapDirectory(FixDirectoryName(dirInfo));
                    }
                }
            }


        }

        // Get file properties
        public static void ProcessFile(string filePath)
        {

            try
            {

                // - File Name
                //Console.WriteLine("File Name '{0}'", Path.GetFileName(filePath));

                // - File Size
                //Console.WriteLine("File Size '{0}'", new System.IO.FileInfo(filePath).Length);

                // - Hash the File
                //Console.WriteLine("MD5 hash '{0}'", checkMD5(filePath)); ;

                // - Get proper file path directory
                //Console.WriteLine("Directory Name '{0}'", Path.GetDirectoryName(filePath));

                // - Finished 
                //Console.WriteLine("Processed file '{0}'", filePath);
                //Log.Debug("Processed file '{0}'", filePath);


                string fileName = Path.GetFileName(filePath);
                Int64 fileLength = Convert.ToInt64(new System.IO.FileInfo(filePath).Length);
                string fullPath = Path.GetDirectoryName(filePath);
                string hash = null;

                //Log.Information("Looks good -> {0}", fileName);
            // Store in the database
                InsertFileDataIntoDatabase(fileName, fileLength, fullPath, hash);
            //InsertFileDataIntoDatabase(Path.GetFileName(filePath), new System.IO.FileInfo(filePath).Length, Path.GetDirectoryName(filePath), checkMD5(filePath));


            }
            catch (Exception ex) 
            { 
                //Log.Information("{0} problem with processing : {1}", ex.ToString(), filePath);
                Log.Warning("Problem with ProcessFile {0}", ex);

            }
            return;
        }

        

        private static void InsertFileDataIntoDatabase(string fileName, long filesize, string directorypath, string hash)
        {
            

            try 
            { 
            // Sets up database connection
            var cs = "Host=localhost;Username=" + Constants.SQLDatabaseUserName + ";Password=" + Constants.SQLDatabasePassword + ";Database=" + Constants.SQLDatabaseName + ";";
            using var con = new NpgsqlConnection(cs);
            con.Open();

            string tempsql = "INSERT INTO file_properties (fileName, filesize, directorypath, hash) " +
                                                "VALUES ('" + fileName + "','" + filesize + "','" + directorypath + "','" + hash + "') " +
                                                "ON CONFLICT " +    // This is for conflicts on unique keys
                                                "DO NOTHING " +
                                                ";";
            var cmd = new NpgsqlCommand(tempsql, con);

            Log.Debug(tempsql);

            cmd.ExecuteNonQuery();

            //Console.WriteLine("{0} rows inserted", );
            }
            catch (Exception ex)
            {
                //Log.Information("{0} problem with processing : {1}", ex.ToString(), filePath);
                Log.Warning("Problem with database insert {0}", ex);

            }
        }

        private static System.IO.DirectoryInfo FixDirectoryName(System.IO.DirectoryInfo directory)
        {
            // Permanently remove apostrophe from directory name
            string newDirectoryPath = directory.FullName.Replace("'", "");
            Directory.Move(directory.FullName, newDirectoryPath);
            return new System.IO.DirectoryInfo(newDirectoryPath);
        }

        private static System.IO.FileInfo FixFileName(System.IO.FileInfo file)
        {
            string newFilePath = file.FullName.Replace("'", "_");

            try
            {
                File.Move(file.FullName, newFilePath);
            }
            catch(Exception ex)
            {
                Log.Error(ex.ToString());
            }

            return new System.IO.FileInfo(newFilePath);
        }

        private static bool ContainsInvalidChacterName(string name)
        {
            return name.Contains("'");
        }

    }
}





/*
 * 
 * try
            {   // Process the list of files found in the directory.
               string[] fileEntries = Directory.GetFiles(targetDirectory);
               foreach (string fileName in fileEntries)
               {
                    ProcessFile(fileName);
               }
               string[] subdirectoryEntries = Directory.GetDirectories(targetDirectory);
               foreach (string subdirectory in subdirectoryEntries)
               {
                    ProcessDirectory(subdirectory);
               }
            }
            catch (Exception ex) { Console.WriteLine("{0} problem with directory : {1}", ex.ToString(), targetDirectory); }


























*/