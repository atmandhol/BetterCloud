using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Threading;
using MongoDB.Driver;
using MongoDB.Bson;

namespace KafkaConsumer
{
    class Program
    {
        static long count = 0;
        static void Main(string[] args)
        {

            var options = new KafkaOptions(new Uri("http://localhost:9092"), new Uri("http://localhost:9093"), new Uri("http://localhost:9094"));
            var router = new BrokerRouter(options);
            var consumer = new Consumer(new ConsumerOptions("test", router));

            var connectionString = "mongodb://localhost:27017"; 
            var client = new MongoClient(connectionString); 
            var db = client.GetDatabase("betterhack"); 
            var col = db.GetCollection<BsonDocument>("messages");

            Console.Write("Start - " + DateTime.Now.ToString());
            BsonDocument doc;

              foreach (var message in consumer.Consume())
                    {
                        count++;
                        if (count % 10000 == 0)
                        {
                            Console.Write("\nInterval - " + count + " reached at " + DateTime.Now.ToString()); // + ". Message - " + message.Value.ToUtf8String()
                        }

                        BsonDocument messageStr = message.ToBsonDocument();
                        
                        //col.InsertOneAsync(doc);
                    }

                Console.Write("End - " + DateTime.Now.ToString());
                Console.Write("\n\nTotal Message Count : " + count);
        }
    }
}
