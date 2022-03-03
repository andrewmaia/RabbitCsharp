using RabbitMQ.Client;
using System;
using System.Text;


namespace Rabbit.Produtor
{
    class Program
    {
        static void Main(string[] args)
        {
            int qtdMsg = int.Parse(args[0]);
            Console.WriteLine($" Quantidade mensagens {qtdMsg} ");

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                //Declaração da fila. Nao é necessário ficar criando caso ela ja tenho sido criada no painel do Rabbit
                channel.QueueDeclare(queue: "filaConsole",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                for (int i=0;i< qtdMsg; i++)
                { 
                    string message = i.ToString("00")+ "-" + Guid.NewGuid().ToString();
                    var body = Encoding.UTF8.GetBytes(message);

                    //Publica utilizando a routingkey filaConsole que por direct exchange(direct é padrão de binding)  manda para a fila de mesmo nome
                    channel.BasicPublish(exchange: "",
                                         routingKey: "filaConsole",
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
