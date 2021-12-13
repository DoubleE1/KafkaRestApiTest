using ksqlDB.RestApi.Client.KSql.Linq;
using ksqlDB.RestApi.Client.KSql.Query.Context;
using ksqlDB.RestApi.Client.KSql.Query.Operators;
using ksqlDB.RestApi.Client.KSql.RestApi;
using ksqlDB.RestApi.Client.KSql.RestApi.Http;
using ksqlDB.RestApi.Client.KSql.RestApi.Serialization;
using ksqlDB.RestApi.Client.KSql.RestApi.Statements;
using Nito.AsyncEx;
using System;
using Topshelf;

namespace KafkaTest
{
    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                HostFactory.Run(x =>
                {
                    x.Service<KSQLService>(s =>
                    {
                        s.ConstructUsing(name => new KSQLService());
                        s.WhenStarted(tc => tc.OnStart());
                        s.WhenStopped(tc => tc.OnStop());
                    });
                    x.RunAsLocalSystem();
                    x.SetDescription("KafkaTest");
                    x.SetDisplayName("KafkaTest");
                    x.SetServiceName("KafkaTest");
                });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        
    }
}
