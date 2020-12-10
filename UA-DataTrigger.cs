using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace DataTriggerFunction
{
    public static class UA_DataTrigger
    {
        [FunctionName("UA_DataTrigger")]
        public static async Task RunAsync([TimerTrigger("2 * * * * *")]TimerInfo myTimer, ILogger log)
        {
        log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            int retrieveAllData = 13;
            string dataName;
            while (retrieveAllData>0)
            {
                String requestString = "https://sep6-ua-weather.azurewebsites.net/";
                try
                {
                    // Call Your  API
                    HttpClient newClient = new HttpClient();
                    if(retrieveAllData>7)
                        requestString += "flights?requestBody=";
                    else if(retrieveAllData>3)
                        requestString += "weather?requestBody=";
                    else
                        requestString += "manufacturer?requestBody=";

                    switch(retrieveAllData)
                    {
                        case 1:
                            dataName = "planes-per-manufacturer";
                            break;
                        case 2:
                            dataName = "flights-per-manufacturer";
                            break;
                        case 3:
                            dataName = "airbus-per-manufaturer";
                            break;
                        case 4:
                            dataName = "wo-origins";
                            break;
                        case 5:
                            dataName = "temp-attributes";
                            break;
                        case 6:
                            dataName = "dewp-attributes";
                            break;
                        case 7:
                            dataName = "avgtemp-origin";
                            break;
                        case 8:
                            dataName = "flights-per-month";
                            break;
                        case 9:
                            dataName = "flights-per-month-stacked";
                            break;
                        case 10:
                            dataName = "top-dest-table";
                            break;
                        case 11:
                            dataName = "top-dest";
                            break;
                        case 12:
                            dataName = "avg-airtime";
                            break;
                        case 13:
                            dataName = "delays";
                            break;
                        default:
                            dataName = "error";
                            break;     
                    }
                    requestString += dataName;
                    HttpRequestMessage newRequest = new HttpRequestMessage(HttpMethod.Get, requestString);
                    //Read Server Response
                    HttpResponseMessage response = await newClient.SendAsync(newRequest);
                    string data = await response.Content.ReadAsStringAsync();

                    /*BLOB*/
                    string blob = dataName + ".txt";
                    string connectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING");
                    BlobServiceClient blobServiceClient = new BlobServiceClient(connectionString);
                    BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("uadata");

                    // Get a reference to a blob
                    BlobClient blobClient = containerClient.GetBlobClient(blob);
                    await blobClient.UploadAsync(GenerateStreamFromString(data), true);
                    retrieveAllData--;
                }
                catch (HttpRequestException e)
                {
                    Console.WriteLine("\nException Caught!");
                    Console.WriteLine("Message :{0} ", e.Message);
                }
               
            }
           
           
        }

        public static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }
    }
}
