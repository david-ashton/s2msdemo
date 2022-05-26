using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient; // dotnet add package MySql.Data

namespace S2dbPersonQuery
{

    public class S2dbPersonQueryWorker
    {
        private string connString;
        public long[] ids;
        string table = S2dbPersonQuery.TABLE;
        Queue q;

        public S2dbPersonQueryWorker( string connString, Queue q ) 
        {
            this.connString = connString;
            this.q = q;
            ids = (long[])S2dbPersonQuery.ids.ToArray(typeof(long));
        }
        public void Worker()
        {
            long qryCount = 0;
            // Create another connection per thread
            using (IDbConnection conn = new MySqlConnection())
            {
                conn.ConnectionString = this.connString;
                conn.Open();

                using (IDbCommand dbCommand = conn.CreateCommand())
                {
                    Stopwatch stop = new Stopwatch();
                    Random rnd = new Random();
                    stop.Start();
                    long personId = 0;
                    while (stop.ElapsedMilliseconds < S2dbPersonQuery.WORKLOAD_TIME * 1000)
                    {
                        personId = (long)ids[rnd.Next(S2dbPersonQuery.ID_COUNT)];
                        dbCommand.CommandText = $"select * from {table} where personid={personId}";
                        dbCommand.ExecuteNonQuery();
                        qryCount++;
                    }
                }
            }
            q.Enqueue(qryCount);
        }
    }


    public class S2dbPersonQuery
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

        public const int ID_COUNT = 1000;

        public const string QRY = "select * from {TABLE} where personid={personId}";

        // The number of workers to run
        //public const int numWorkerThreads = 2;
        // Run the workload for this many seconds
        public const int WORKLOAD_TIME = 10; // seconds

        /**
        * Internal code starts here
        * #########################
        */

        private IDbCommand dbCommand;
        private Queue q = new Queue(2000);
        public static ArrayList ids = new ArrayList();

        public S2dbPersonQuery()
        {
            IDbConnection conn = new MySqlConnection();
            conn.ConnectionString = $"Server={HOST};Port={PORT};Uid={USER};Pwd={PASSWORD};";
            conn.Open();
            dbCommand = conn.CreateCommand();
            dbCommand.CommandText = $"USE {DATABASE}";
            dbCommand.ExecuteNonQuery();
            Console.WriteLine("Getting IDs for query workload");

            dbCommand.CommandText = $"select personid from {TABLE} order by personid limit {ID_COUNT}";

            using (IDataReader reader = dbCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    ids.Add((long)reader["personid"]);
                }
                Console.WriteLine("IDS Read: " + ids.Count);
            }
        }

        private void DoBenchmark( int numWorkerThreads )
        {
            string connString = $"Server={HOST};Port={PORT};database={DATABASE};Uid={USER};Pwd={PASSWORD};SslMode=None;";
            Console.WriteLine(new String('=', $"Launching {numWorkerThreads} workers for {WORKLOAD_TIME} sec".Length));
            Console.WriteLine($"Launching {numWorkerThreads} workers for {WORKLOAD_TIME} sec");
            Thread[] workers = new Thread[numWorkerThreads];
            for (int i = 0; i < numWorkerThreads; i++)
            {
                workers[i] = new Thread(new ThreadStart(new S2dbPersonQueryWorker(connString,q).Worker));
                workers[i].Start();
            }
            Console.WriteLine($"{workers.Length} workers running...");
            for (int i = 0; i < numWorkerThreads; i++)
            {
                workers[i].Join();
            }
            ShowStats( numWorkerThreads );
        }


        private void ShowStats( int numWorkerThreads )
        {
            long count = 0;
            long tcount = 0;
            while (q.Count > 0)
            {
                tcount = (long)q.Dequeue();
                Console.WriteLine($"{tcount} read from queue");
                count += tcount;
            }
            Console.WriteLine($"{count} queries using {numWorkerThreads} workers");
            Console.WriteLine("Average Latency: " + Math.Round(WORKLOAD_TIME*1000.000 * numWorkerThreads / count , 3)+ " ms");
            Console.WriteLine($"{count / WORKLOAD_TIME} queries per second/QPS");
        }


        public static int Main(string[] args)
        {
            S2dbPersonQuery tester = new S2dbPersonQuery();
            try
            {
                tester.DoBenchmark(1);
                tester.DoBenchmark(2);
                tester.DoBenchmark(3);
                tester.DoBenchmark(5);
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