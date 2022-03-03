using RabbitMQ.Client;
using System;
using System.Text;

namespace Rabbit.Produtor.Direct
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

                //decalração do exchange do tipo direct. Nao é necessário ficar declarando caso ele ja tenho sido criada no painel do Rabbit
                channel.ExchangeDeclare("exchangeDirect", type: "direct");

                //Bind: Amarra as filas com o exchange

                //Na fila 1 foram feito dos binds direct:

                //Na fila 1 ela irá receber qualquer mensagem que for enviada com exatamente a routingKey 'rota.fila1'
                channel.QueueBind("fila1", exchange: "exchangeDirect", routingKey: "rota.fila1");
                //Na fila 1 ela irá receber qualquer mensagem que for enviada com exatamente a routingKey 'rota.filaExtra'
                channel.QueueBind("fila1", exchange: "exchangeDirect", routingKey: "rota.filaExtra");


                //Na fila 2 ela irá receber qualquer mensagem que for enviada com exatamente a routingKey 'rota.fila2'
                channel.QueueBind("fila2", exchange: "exchangeDirect", routingKey: "rota.fila2");

                for (int i = 0; i < qtdMsg; i++)
                {
                    string message = i.ToString("00") + "-" + Guid.NewGuid().ToString();
                    var body = Encoding.UTF8.GetBytes(message);

                    //Publica a msg utilizando o exchange declarado

                    //Esta mensagem será enviada a fila1 
                    channel.BasicPublish(exchange: "exchangeDirect",
                                         routingKey: "rota.fila1",
                                         basicProperties: null,
                                         body: body);

                    //Repare que esta mensagem será enviada a fila1 também pois há outra bind para a fila1 
                    channel.BasicPublish(exchange: "exchangeDirect",
                                         routingKey: "rota.filaExtra",
                                         basicProperties: null,
                                         body: body);

                    //Esta mensagem será enviada a fila2 
                    channel.BasicPublish(exchange: "exchangeDirect",
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
