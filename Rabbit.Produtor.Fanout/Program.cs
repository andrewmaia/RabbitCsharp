using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Text;

namespace Rabbit.Produtor.Fanout
{
    class Program
    {
        //Exemplo de exemplo de fanout
        static void Main(string[] args)
        {
            int qtdMsg = 10;
            Console.WriteLine($" Quantidade mensagens {qtdMsg} ");

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                //Declaração da fila. Nao é necessário ficar criando caso ela ja tenho sido criada no painel do Rabbit

                //fila1
                channel.QueueDeclare(queue: "fila1",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //fila2
                channel.QueueDeclare(queue: "fila2",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //decalração do exchange. Nao é necessário ficar declarando caso ele ja tenho sido criada no painel do Rabbit
                channel.ExchangeDeclare("exchangeTeste", type: "fanout");

                //Bind: Amarra a fila com o exchange
                channel.QueueBind("fila1", exchange: "exchangeTeste", routingKey:"");
                channel.QueueBind("fila2", exchange: "exchangeTeste", routingKey: "");

                for (int i = 0; i < qtdMsg; i++)
                {
                    string message = i.ToString("00") + "-" + Guid.NewGuid().ToString();
                    var body = Encoding.UTF8.GetBytes(message);

                    //Publica utilizando o exchange declarado 
                    channel.BasicPublish(exchange: "exchangeTeste",
                                         routingKey: "",
                                         basicProperties: null,
                                         body: body);

                    Console.WriteLine($" {message} Enviada ");
                    System.Threading.Thread.Sleep(1000);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine(); 
        }
    }
}
