using System.Net.WebSockets;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;

namespace GremlinNetSample
{
    /// <summary>
    /// Sample program that shows how to get started with the Graph (Gremlin) APIs for Azure Cosmos DB using the open-source connector Gremlin.Net
    /// Using this as a guide from: https://docs.microsoft.com/en-us/samples/azure-samples/azure-cosmos-db-graph-gremlindotnet-getting-started/azure-cosmos-db-graph-gremlindotnet-getting-started/
    // Note that this example uses Gremlin.Net 3.4.9.  This will not work with the Gremlin.Net 3.5.x
    /// </summary>
    class Program
    {
        // Azure Cosmos DB Configuration variables
        // Replace the values in these variables to your own.
        // <configureConnectivity>
        private static string Host => Environment.GetEnvironmentVariable("Host") ?? throw new ArgumentException("Missing env var: Host");
        private static string PrimaryKey => Environment.GetEnvironmentVariable("PrimaryKey") ?? throw new ArgumentException("Missing env var: PrimaryKey");
        private static string Database = "graphmas";
        private static string Container = "holidays";
        private static bool EnableSSL
        {
            get
            {
                if (Environment.GetEnvironmentVariable("EnableSSL") == null)
                {
                    return true;
                }

                if (!bool.TryParse(Environment.GetEnvironmentVariable("EnableSSL"), out bool value))
                {
                    throw new ArgumentException("Invalid env var: EnableSSL is not a boolean");
                }

                return value;
            }
        }

        private static int Port
        {
            get
            {
                if (Environment.GetEnvironmentVariable("Port") == null)
                {
                    return 443;
                }

                if (!int.TryParse(Environment.GetEnvironmentVariable("Port"), out int port))
                {
                    throw new ArgumentException("Invalid env var: Port is not an integer");
                }

                return port;
            } 
        }

        // </configureConnectivity>

        // Gremlin queries that will be executed.
        // <defineQueries>
        private static Dictionary<string, string> gremlinQueries = new Dictionary<string, string>
        {            
            { "Get Vertex Count", "g.V().count()" },
            { "Get Unique Vertex Labels", "g.V().label().dedup()" },
            { "Get Edge Count", "g.E().count()" },
            { "Get Unique Edge Labels", "g.E().label().dedup()" },
            { "Get All Vertices", "g.V()"},
            { "Get All Holiday Vertices","g.V().hasLabel('Holiday')"},
            { "Get All Country Vertices","g.V().hasLabel('Country')"},
            { "Get All Edges", "g.E()"},
            { "Get First Vertex", "g.V().limit(1)"},
            { "Get All Properties and Values for a Holiday vertex", "g.V().hasLabel('Holiday').limit(1).valueMap()"},
            { "Get All Vertices with a holidayDate property","g.V().has('holidayDate')"},
            { "Get All Vertices without a holidayDate property","g.V().hasNot('holidayDate')"},
            { "Get the Number of Holidays by Date","g.V().hasLabel('Holiday').group().by('holidayDate').by(values('name').count())"},
            { "Get the Holidays Ordered by Date, then Name", "g.V().hasLabel('Holiday').order().by('holidayDate').by('name').valueMap('name','holidayDate')"},
            { "Get Holidays Between Colombia and Mexico","g.V().has('name','Colombia').outE().inV().inE().outV().has('name','Mexico')" },
            { "Get Holidays Between Colombia and Mexico - Path","g.V().has('name','Colombia').outE().inV().inE().outV().has('name','Mexico').path()" },
            { "Get Holidays Between Colombia and Mexico - Execution Profile","g.V().has('name','Colombia').outE().inV().inE().outV().has('name','Mexico').executionprofile()" }
        };
        // </defineQueries>

        // Starts a console application that executes every Gremlin query in the gremlinQueries dictionary. 
        static void Main(string[] args)
        {
            // <defineClientandServerObjects>
            string containerLink = "/dbs/" + Database + "/colls/" + Container;
            Console.WriteLine($"Connecting to: host: {Host}, port: {Port}, container: {containerLink}, ssl: {EnableSSL}");
            var gremlinServer = new GremlinServer(Host, Port, enableSsl: EnableSSL, 
                                                    username: containerLink, 
                                                    password: PrimaryKey);

            ConnectionPoolSettings connectionPoolSettings = new ConnectionPoolSettings()
            {
                MaxInProcessPerConnection = 10,
                PoolSize = 30, 
                ReconnectionAttempts= 3,
                ReconnectionBaseDelay = TimeSpan.FromMilliseconds(500)
            };

            var webSocketConfiguration =
                new Action<ClientWebSocketOptions>(options =>
                {
                    options.KeepAliveInterval = TimeSpan.FromSeconds(10);
                });


            using (var gremlinClient = new GremlinClient(
                gremlinServer, 
                new GraphSON2Reader(), 
                new GraphSON2Writer(), 
                GremlinClient.GraphSON2MimeType, 
                connectionPoolSettings, 
                webSocketConfiguration))
            {
            // </defineClientandServerObjects>
            int selection = 1;
            while (selection > 0 && selection <= gremlinQueries.Count){
                PrintMenu(gremlinQueries);                               
                Console.WriteLine("What query would you like to run? Select 0 to quit.");
                selection = Convert.ToInt16(Console.ReadLine());
                if (selection == 0)
                    continue;
                ProcessMenu(selection,gremlinQueries,gremlinClient);
            }
            // Exit program
            Console.WriteLine("Done. Press any key to exit...");
            Console.ReadLine();
            }
        }
        private static Task<ResultSet<dynamic>> SubmitRequest(GremlinClient gremlinClient, KeyValuePair<string, string> query)
        {
            try
            {
                return gremlinClient.SubmitAsync<dynamic>(query.Value);
            }
            catch (ResponseException e)
            {
                Console.WriteLine("\tRequest Error!");

                // Print the Gremlin status code.
                Console.WriteLine($"\tStatusCode: {e.StatusCode}");

                // On error, ResponseException.StatusAttributes will include the common StatusAttributes for successful requests, as well as
                // additional attributes for retry handling and diagnostics.
                // These include:
                //  x-ms-retry-after-ms         : The number of milliseconds to wait to retry the operation after an initial operation was throttled. This will be populated when
                //                              : attribute 'x-ms-status-code' returns 429.
                //  x-ms-activity-id            : Represents a unique identifier for the operation. Commonly used for troubleshooting purposes.
                PrintStatusAttributes(e.StatusAttributes);
                Console.WriteLine($"\t[\"x-ms-retry-after-ms\"] : { GetValueAsString(e.StatusAttributes, "x-ms-retry-after-ms")}");
                Console.WriteLine($"\t[\"x-ms-activity-id\"] : { GetValueAsString(e.StatusAttributes, "x-ms-activity-id")}");

                throw;
            }
        }
        private static void PrintMenu(Dictionary<string,string> queries)
        {
            Console.WriteLine("Gremlin Queries");
            Console.WriteLine("===============");
            for(int i = 0; i < queries.Keys.Count(); i++){
                Console.WriteLine($"{i+1}. {queries.Keys.ElementAt(i)}");
            }
        }
        private static void ProcessMenu(int queryNumber, Dictionary<string,string> queries, GremlinClient gremlinClient)
        {
            if (queryNumber > queries.Keys.Count)
            {
                Console.WriteLine("No such query. Game over.");
            } else {
                var query = queries.ElementAt(queryNumber-1);
                
                var resultSet = SubmitRequest(gremlinClient, query).Result;
                if (resultSet.Count > 0)
                    {
                        Console.WriteLine($"\tQuery: {query}");
                        Console.WriteLine("\tResult:");
                        foreach (var result in resultSet)
                        {
                            // The vertex results are formed as Dictionaries with a nested dictionary for their properties
                            string output = JsonConvert.SerializeObject(result);
                            Console.WriteLine($"\t{output}");
                        }
                        Console.WriteLine();                        
                    }
                    else {
                        Console.WriteLine("No Results");
                    }
                    // Print the status attributes for the result set.
                    // This includes the following:
                    //  x-ms-status-code            : This is the sub-status code which is specific to Cosmos DB.
                    //  x-ms-total-request-charge   : The total request units charged for processing a request.
                    //  x-ms-total-server-time-ms   : The total time executing processing the request on the server.
                    PrintStatusAttributes(resultSet.StatusAttributes);
                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue");
                    Console.ReadKey();
            }
        }
        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
        {
            Console.WriteLine($"\tStatusAttributes:");
            Console.WriteLine($"\t[\"x-ms-status-code\"] : { GetValueAsString(attributes, "x-ms-status-code")}");
            Console.WriteLine($"\t[\"x-ms-total-server-time-ms\"] : { GetValueAsString(attributes, "x-ms-total-server-time-ms")}");
            Console.WriteLine($"\t[\"x-ms-total-request-charge\"] : { GetValueAsString(attributes, "x-ms-total-request-charge")}");
        }

        public static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(GetValueOrDefault(dictionary, key));
        }

        public static object GetValueOrDefault(IReadOnlyDictionary<string, object> dictionary, string key)
        {         
            if (dictionary.ContainsKey(key))
            {
                return dictionary[key];
            }

            return null;
        }
    }
}