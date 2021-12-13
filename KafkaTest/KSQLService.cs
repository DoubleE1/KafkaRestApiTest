using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaTest
{
    public class KSQLService
    {
        private List<Task> runningTask;
        public void OnStart()
        {
            try
            {
                KsqlRestHelper helper = new KsqlRestHelper();
                runningTask = new List<Task>();

                KsqlRestHelper ksqlRestHelper = new KsqlRestHelper();

                //runningTask.Add(Task.Factory.StartNew(() => ksqlRestHelper.SubscribeAsync(), TaskCreationOptions.LongRunning));
                runningTask.Add(Task.Factory.StartNew(() => ksqlRestHelper.PushQueryAsync("SELECT * FROM MyClasses EMIT CHANGES;", KsqlRestHelper.OffSetType.Earliest), TaskCreationOptions.LongRunning));

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public void OnStop()
        {
            Console.WriteLine("Service stopped.");
        }
    }
}
