using RabbitMQ.Client;
using System;
using System.Text;


namespace Rabbit.Produtor.Topic
{
    class Program
    {
        static void Main(string[] args)
        {
            int qtdMsg = 10;
            Console.WriteLine($" Quantidade mensagens {qtdMsg} ");

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            using (IModel channel = connection.CreateModel())
            {
                //Declaração da fila. Nao é necessário ficar criando caso ela ja tenho sido criada no painel do Rabbit

                //filaPrincipal
                channel.QueueDeclare(queue: "filaPrincipal",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //Fila1
                channel.QueueDeclare(queue: "fila1",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //Fila2
                channel.QueueDeclare(queue: "fila2",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //decalração do exchange do tipo topic. Nao é necessário ficar declarando caso ele ja tenho sido criada no painel do Rabbit
                channel.ExchangeDeclare("exchangeTesteTopic", type: "topic");

                //Bind: Amarra as filas com o exchange
                //Na fila principal ela irá receber qualquer mensagem que for enviada com a routingKey que comece com 'rota.'
                channel.QueueBind("filaPrincipal", exchange: "exchangeTesteTopic", routingKey: "rota.#");
                //Na fila 1 ela irá receber qualquer mensagem que for enviada com a routingKey 'rota.fila1'
                channel.QueueBind("fila1", exchange: "exchangeTesteTopic", routingKey: "rota.fila1");
                //Na fila 2 ela irá receber qualquer mensagem que for enviada com a routingKey 'rota.fila2'
                channel.QueueBind("fila2", exchange: "exchangeTesteTopic", routingKey: "rota.fila2");

                for (int i = 0; i < qtdMsg; i++)
                {
                    string message = i.ToString("00") + "-" + Guid.NewGuid().ToString();
                    var body = Encoding.UTF8.GetBytes(message);

                    //Publica a msg utilizando o exchange declarado
                    
                    //Repare que esta mensagem será enviada a fila1 e também a filaPrincipal devido as suas routingKeys
                    channel.BasicPublish(exchange: "exchangeTesteTopic",
                                         routingKey: "rota.fila1",
                                         basicProperties: null,
                                         body: body);

                    //Repare que esta mensagem será enviada a fila2 e também a filaPrincipal devido as suas routingKeys
                    channel.BasicPublish(exchange: "exchangeTesteTopic",
                                         routingKey: "rota.fila2",
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
