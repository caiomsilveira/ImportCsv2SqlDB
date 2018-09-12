using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Configuration;
using System.IO;
using System.Linq;

namespace ImportCsv2SqlDB
{
    class Program
    {
        static void Main(string[] args)
        {
            string csvFolder = ConfigurationManager.AppSettings["CSV_FOLDER"].ToString();
            string csvSeparator = ConfigurationManager.AppSettings["CSV_SEPARATOR"].ToString();
            

            var start = DateTime.Now;
            Console.WriteLine($"Start import process for csv folder {csvFolder} at {DateTime.Now.ToString()}");
            
            foreach (string file in Directory.GetFiles(csvFolder, "*.csv"))
            {
                try
                {
                    string fileName = GetFileName(file);
                    string tableName = fileName.Replace(".csv", "").Replace(".CSV", "");
                    Console.WriteLine($"File: { fileName } starting import at {DateTime.Now.ToString()}");

                    string columnLine = null;
                    using (StreamReader sr = new StreamReader(file))
                    {
                        columnLine = sr.ReadLine();
                        sr.Close();
                        sr.Dispose();
                    }
                    var colNames = columnLine.Split(csvSeparator[0]);
                    
                    for(int i = 0; i < colNames.Length; i ++)
                    {
                        if (string.IsNullOrEmpty(colNames[i]))
                        {
                            colNames[i] = $"[Col{(i + 1).ToString()}]";
                        }
                        else
                        {
                            colNames[i] = $"[{colNames[i]}]";
                        }
                    }

                    string createTableCommand = $@"if(exists (select 1 from sys.tables where name = '{tableName}')) drop table dbo.[{tableName}];
                                                   create table dbo.[{tableName}] ({string.Join("varchar(max) null, ", colNames)} varchar(max) null );";

                    ExecuteCommand(createTableCommand);
                    Console.WriteLine($"Table created : { tableName }");
                    Console.WriteLine($"Inserting data ... ");


                    string bulkCopyCommand = $@"BULK INSERT dbo.[{tableName}]
                                                    FROM '{file}'
                                                    WITH
                                                    (
                                                        FIRSTROW = 2,
                                                        FIELDTERMINATOR = ',',
                                                        ROWTERMINATOR = '\n',
                                                        BATCHSIZE = 100000
                                                    )";

                    ExecuteCommand(bulkCopyCommand);
                    Console.WriteLine($"Data Inserted.");
                    Console.WriteLine($"File: { fileName } finished import at {DateTime.Now.ToString()}");
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

                
            }

            Console.WriteLine($" Import process finished import at {DateTime.Now.ToString()}");
            Console.WriteLine($" Time eliped: {(DateTime.Now - start).ToString()}");
            Console.ReadKey();
        }

        private static string GetFileName(string file)
        {
            return file.Substring(file.LastIndexOf(@"\") + 1);
        }
        
        public static void ExecuteCommand(string CMD)
        {
            SqlConnection conn = new SqlConnection(ConfigurationManager.AppSettings["CONNECTION_STRING"].ToString());
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandTimeout = 600;
            cmd.CommandText = CMD;

            try
            {
                conn.Open();
                cmd.ExecuteScalar();
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
