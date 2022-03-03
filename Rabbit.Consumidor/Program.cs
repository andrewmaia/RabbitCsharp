using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Linq;
using System.Text;

namespace Rabbit.Consumidor
{
    class Program
    {
        static int tempoEspera;
        static bool autoAck;
        static void Main(string[] args)
        {
            tempoEspera = int.Parse(args[0]);
            autoAck = bool.Parse(args[1]);
            Console.WriteLine($" Tempo de Espera {tempoEspera} ms");
            Console.WriteLine($" AutoAck {autoAck}");

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += Receber;
                
                //associa o consumer ao chanel e a fila
                //Ver explicação autoack no word                
                channel.BasicConsume(queue: "filaConsole", autoAck: autoAck, consumer: consumer);

                //Esta é opção de prefetch que impede que o round robin distribua igualmente as mensagens para os consumidores
                //Ver explicação no word
                channel.BasicQos(0, 1, true);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        static void Receber(object sender, BasicDeliverEventArgs ea)
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body.ToArray());
            Console.WriteLine(" [x] Received {0}", message);
            System.Threading.Thread.Sleep(tempoEspera);
            if (!autoAck)
            {
                EventingBasicConsumer model = (EventingBasicConsumer)sender;
                model.Model.BasicAck(ea.DeliveryTag, false);
            }
            //Se der erro utilizar nack para devolver a msg: model.Model.BasicNack(ea.DeliveryTag, false,false);
            //Ver explicação autoack no word
        }
    }
}


