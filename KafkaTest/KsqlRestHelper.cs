using ksqlDB.RestApi.Client.KSql.Linq;
using ksqlDB.RestApi.Client.KSql.Query.Context;
using ksqlDB.RestApi.Client.KSql.RestApi;
using ksqlDB.RestApi.Client.KSql.RestApi.Http;
using ksqlDB.RestApi.Client.KSql.RestApi.Serialization;
using ksqlDB.RestApi.Client.KSql.RestApi.Statements;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace KafkaTest
{
    public class KsqlRestHelper
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        public async void PushQueryAsync(string ksql, OffSetType offSetType = OffSetType.Latest)
        {
            try
            {
                #region WebRequest
                //HttpWebRequest req;
                //HttpWebResponse res = null;
                //req = (HttpWebRequest)WebRequest.Create("http://localhost:8088/query");
                //req.Method = "POST";
                //req.ContentType = "application/vnd.ksql.v1+json";
                //req.Accept = "application/vnd.ksql.v1+json";

                //var reqJobj = new KsqlRequest() { ksql = ksql, streamsProperties = new StreamsProperties() { OffSetType = offSetType.ToString().ToLower() } };

                //var byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(reqJobj));
                //var reqStream = req.GetRequestStream();
                //reqStream.Write(byteArray, 0, byteArray.Length);
                //reqStream.Close();

                //res = (HttpWebResponse)req.GetResponse();
                //var stream = res.GetResponseStream();
                #endregion


                #region HttpClient
                var httpClient = new HttpClient()
                {
                    BaseAddress = new Uri("http://localhost:8088/query")
                };

                var reqJobj = new
                {
                    sql = ksql,
                    properties = new Dictionary<string, string>() { { "auto.offset.reset", "earliest" } },
                };
                var json = JsonConvert.SerializeObject(reqJobj);
                var data = new StringContent(json, Encoding.UTF8, "application/json");
                var contentType = "application/vnd.ksqlapi.delimited.v1";
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue(contentType));

                var httpRequestMessage = new HttpRequestMessage(HttpMethod.Post, "query-stream")
                {
                    Content = data
                };
                httpRequestMessage.Version = HttpVersion.Version20;
                httpRequestMessage.VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher;

                var httpResponseMessage = await httpClient.SendAsync(httpRequestMessage,
                    HttpCompletionOption.ResponseHeadersRead);

                var stream = await httpResponseMessage.Content.ReadAsStreamAsync();
                #endregion

                var streamReader = new StreamReader(stream);
                while (!streamReader.EndOfStream)
                {
                    var rawData = streamReader.ReadLine();
                    Console.WriteLine($"{DateTime.Now.ToString("hh:mm:ss")} - Raw data received: {rawData}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw ex.InnerException;
            }
            finally
            {
                //if (res != null)
                //    res.Close();
            }

        }

        public async void SubscribeAsync()
        {
            var ksqlDbUrl = @"http:\\localhost:8088";
            var contextOptions = new KSqlDBContextOptions(ksqlDbUrl);
            await using var context = new KSqlDBContext(contextOptions);

            EntityCreationMetadata metadata = new EntityCreationMetadata
            {
                KafkaTopic = "MyClass",
                Partitions = 1,
                Replicas = 1,
                ValueFormat = SerializationFormats.Json
            };

            var httpClientFactory = new HttpClientFactory(new Uri(ksqlDbUrl));
            var restApiClient = new KSqlDbRestApiClient(httpClientFactory);

            var httpResponseMessage = await restApiClient.CreateStreamAsync<MyClass>(metadata);
            Console.WriteLine($"CreateStreamAsync: {httpResponseMessage.IsSuccessStatusCode} - {httpResponseMessage.StatusCode}");

            var from = new TimeSpan(11, 15, 0);
            var to = new TimeSpan(15, 0, 0);

            var query = context.CreateQueryStream<MyClass>()
                .WithOffsetResetPolicy(ksqlDB.RestApi.Client.KSql.Query.Options.AutoOffsetReset.Earliest)
                .Select(c => new { c.Ts, c.Dt });
            //.Where(c => c.Ts.Between(from, to))
            Console.WriteLine(query.ToQueryString());

            var result = query.SubscribeAsync(z =>
            {
                Console.WriteLine($"{z.Ts} - {z.Dt}");
            },
            onError: ex =>
            {
                Console.WriteLine($"Exception: {ex.Message}");
            },
            onCompleted: () =>
            {
                Console.WriteLine("Completed");
            });

            //var value = new MyClass
            //{
            //    UserId = new Random().Next(1, 10),
            //    DeviceId = new Random().Next(1, 999).ToString().PadLeft(3, '0'),
            //    Weather = Summaries[new Random().Next(0, Summaries.Length - 1)],
            //    Dt = DateTime.UtcNow,
            //    Ts = new TimeSpan(1, 2, 3),
            //    DtOffset = new DateTimeOffset(2021, 7, 4, 13, 29, 45, 447, TimeSpan.FromHours(4))
            //};

            //httpResponseMessage = await restApiClient.InsertIntoAsync(value);
            //Console.WriteLine($"InsertIntoAsync: {httpResponseMessage.IsSuccessStatusCode} - {httpResponseMessage.StatusCode}");
        }

        public class MyClass
        {
            public int UserId { get; set; }
            public string DeviceId { get; set; }
            public string Weather { get; set; }
            public DateTime Dt { get; set; }
            public TimeSpan Ts { get; set; }
            public DateTimeOffset DtOffset { get; set; }

            //Note: property with the method 'set' will not be generated in the ksql statement
            //public long UnixDt { get { return (long)this.Dt.ToUniversalTime().Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds; } }
        }

        public class KsqlRequest
        {
            public string ksql { get; set; }
            public StreamsProperties streamsProperties { get; set; }
        }

        public class StreamsProperties
        {
            [JsonProperty("ksql.streams.auto.offset.reset")]
            public string OffSetType { get; set; }
        }

        public enum OffSetType
        {
            Latest = 0,
            Earliest = 1,
        }
    }
}
