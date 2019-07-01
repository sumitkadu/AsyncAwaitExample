using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;

namespace AsyncAwaitExample
{
    class Program
    {
        private static List<string> names = new List<string>() { "A", "B", "C", "D", "E" };
        static void Main(string[] args)
        {
            Console.WriteLine("Saving names...");

            //Without Retry
            //Stopwatch stopwatch = new Stopwatch();
            //stopwatch.Start();
            //SaveNames();
            //stopwatch.Stop();
            //Console.WriteLine($"Elapsed Miliseconds: {stopwatch.ElapsedMilliseconds}");

            //With Retry
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            SaveNames(names);
            stopwatch.Stop();
            Console.WriteLine($"Elapsed Miliseconds: {stopwatch.ElapsedMilliseconds}");
            Console.ReadKey();
        }

        private static void SaveNames(List<string> names)
        {
            SaveNamesAsync();            
        }

        private static async void SaveNamesAsync()
        {
            Parallel.ForEach<string>(names, async (name) =>
            {
                try
                {
                    Console.WriteLine($"Saving name: {name}");                    

                    var maxRetryAttempts = 3;
                    var pauseBetweenFailures = TimeSpan.FromSeconds(2);

                    var retryPolicy = Policy
                        .Handle<SqlException>()
                        .WaitAndRetryAsync(maxRetryAttempts, i => pauseBetweenFailures);

                    await retryPolicy.ExecuteAsync(async () =>
                    {
                        await InsertIntoDB(name);
                    });

                    Console.WriteLine($"Name saved successfully: {name}");
                }
                catch(Exception ex)
                {
                    Console.WriteLine($"Name save failed: {ex.Message}");
                }
            });
        }

        public static async Task InsertIntoDB(string name)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection("Put your connection string...."))
                {
                    connection.Open();

                    string sql = "SELECT MAX(ID) FROM SUMIT_TEST";
                    SqlCommand command = new SqlCommand(sql, connection);
                    int ID = Convert.ToInt32(command.ExecuteScalar()) + 1;
                    Console.WriteLine($"Next available ID {ID}, for Name: {name}");
                    sql = $"INSERT INTO SUMIT_TEST(ID,Name) VALUES({ID},'{name}')";
                    Console.WriteLine($"Insert query for Name: {name} is: {sql}");
                    command = new SqlCommand(sql, connection);
                    command.ExecuteNonQuery();
                    connection.Close();

                }
            }
            catch(SqlException ex)
            {
                Console.WriteLine($"Name save failed: {ex.Message}");
                throw ex;
            }
        }
    }
}
