using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient; // dotnet add package MySql.Data

namespace S2dbPersonInsert
{
    public class Person
    {
        //
        public string personid { get; set; }
        public string firstName { get; set; }
        public string lastName { get; set; }
        public string city { get; set; }
        public string state { get; set; }
        public string zip { get; set; }
        public int likesSports { get; set; }
        public int likesTheatre { get; set; }
        public int likesConcerts { get; set; }
        public int likesVegas { get; set; }
        public int likesCruises { get; set; }
        public int likesTravel { get; set; }

        public Person(string line)
        {
            try
            {
                string[] cols = line.Split(',');
                personid = cols[0];
                firstName = cols[1];
                lastName = cols[2];
                city = cols[3];
                state = cols[4];
                zip = cols[5];
                likesSports = int.Parse(cols[6]);
                likesTheatre = int.Parse(cols[7]);
                likesConcerts = int.Parse(cols[8]);
                likesVegas = int.Parse(cols[9]);
                likesCruises = int.Parse(cols[10]);
                likesTravel = int.Parse(cols[11]);
            }
            catch (Exception ex)
            {
                Console.WriteLine("***ERROR*** exception while parsing line - " + ex.ToString());
                throw;
            }
        }

        public string getPrintable()
        {
            return $"personid: {personid}, firstName: {firstName}, lastName: {lastName}, city: {city}, state: {state}, zip: {zip}, likes: {likesSports} {likesTheatre} {likesConcerts} {likesVegas} {likesCruises} {likesTravel}";
        }

        public string getValuesClause()
        {
            return $"({personid},\"{firstName}\",\"{lastName}\",\"{city}\",\"{state}\",\"{zip}\",{likesSports},{likesTheatre},{likesConcerts},{likesVegas},{likesCruises},{likesTravel}) ";
        }
    }

    public class S2dbPersonInsertWorker
    {
        private string connString;
        string QRY_INSERT = S2dbPersonInsert.QRY_INSERT.Replace("{TABLE}", S2dbPersonInsert.TABLE);
        Queue q;
        string filename;

        public S2dbPersonInsertWorker(string connString, Queue q, string filename)
        {
            this.connString = connString;
            this.q = q;
            this.filename = filename;
        }
        public void Worker()
        {
            long counter = 0;
            // Create another connection per thread
            using (IDbConnection conn = new MySqlConnection())
            {
                conn.ConnectionString = this.connString;
                conn.Open();

                using (IDbCommand dbCommand = conn.CreateCommand())
                {
                    string line;
                    string values = "";
                    System.IO.StreamReader file =
                        new System.IO.StreamReader(filename);
                    while ((line = file.ReadLine()) != null)
                    {
                        if (line.StartsWith("personid,firstname,")) {
                            continue;
                        }
                        counter++;

                        if (values.Length == 0)
                        {
                            values = (new Person(line)).getValuesClause();
                        }
                        else
                        {
                            values += ","+(new Person(line)).getValuesClause();
                        }
                        if ((counter % S2dbPersonInsert.BATCH_SIZE) == 0)
                        {
                            dbCommand.CommandText = QRY_INSERT + values + ";";
                            dbCommand.ExecuteNonQuery();
                            values = "";
                        }
                    }
                    if (values.Length > 0)
                    {
                        dbCommand.CommandText = QRY_INSERT + values + ";";
                        dbCommand.ExecuteNonQuery();
                    }
                    file.Close();
                }
            }
            q.Enqueue(counter);
        }
    }


    public class S2dbPersonInsert
    {
        /**
        * Tweak the following globals to fit your environment
        * ###################################################
        */
        public const string HOST = "172.31.57.177";
        public const int PORT = 3306;
        public const string USER = "root";
        public const string PASSWORD = "dashtontest";

        // Specify which database and table to work with.
        // Note: this database will be dropped at the end of this script
        public const string DATABASE = "msdemo";
        public const string TABLE = "person_rs";

        public const string QRY_INSERT = "insert into {TABLE} ( personid, first_name, last_name, city, state, zip, likes_sports, likes_theatre, likes_concerts, likes_vegas, likes_cruises, likes_travel ) values ";

        public const string CSV_DIR = "C:\\DATA\\person\\csv24\\20M";
        public const int BATCH_SIZE = 1000;

        /**
        * Internal code starts here
        * #########################
        */

        private Queue q = new Queue(2000);

        private void DoBenchmark(string[] filenames)
        {
            int numWorkerThreads = filenames.Length;
            string connString = $"Server={HOST};Port={PORT};database={DATABASE};Uid={USER};Pwd={PASSWORD};SslMode=None;";
            Console.WriteLine(new String('=', $"Launching {numWorkerThreads} workers for files from {CSV_DIR} directory".Length));
            Console.WriteLine($"Launching {numWorkerThreads} workers for files from {CSV_DIR} directory");
            Thread[] workers = new Thread[numWorkerThreads];

            DateTime startTime = DateTime.Now;

            for (int i = 0; i < numWorkerThreads; i++)
            {
                workers[i] = new Thread(new ThreadStart(new S2dbPersonInsertWorker(connString, q, filenames[i]).Worker));
                workers[i].Start();
            }
            Console.WriteLine($"{workers.Length} workers running...");
            for (int i = 0; i < numWorkerThreads; i++)
            {
                workers[i].Join();
            }

            DateTime endTime = DateTime.Now;
            double durationSeconds = (endTime - startTime).TotalSeconds;
            ShowStats(numWorkerThreads, durationSeconds);
        }


        private void ShowStats(int numWorkerThreads, double durationSeconds)
        {
            long count = 0;
            long tcount = 0;
            while (q.Count > 0)
            {
                tcount = (long)q.Dequeue();
                Console.WriteLine($"{tcount} read from queue");
                count += tcount;
            }
            string countFmt = String.Format("{0:n0}", count);
            string insertRate = String.Format("{0:n0}", Math.Round(count / durationSeconds, 0));
            Console.WriteLine($"{countFmt} inserts using {numWorkerThreads} workers in {durationSeconds} seconds");
            Console.WriteLine($"{insertRate} inserts per second");
        }


        public static int Main(string[] args)
        {
            S2dbPersonInsert tester = new S2dbPersonInsert();

            try
            {
                string[] fileEntries = Directory.GetFiles(CSV_DIR);
                tester.DoBenchmark(fileEntries);
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR: {ex.Message}, {ex.GetType()}, {ex.StackTrace}");
                try
                {
                }
                catch
                {
                    // ignore error
                }
                return 1;
            }
        }

    }
}