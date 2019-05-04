using Bond;
using Bond.IO.Safe;
using Bond.Protocols;
using Grpc.Core;
using Grpc.Core.Utils;
using System;
using System.Threading.Tasks;

namespace GrpcConsoleServer
{
    [Schema]
    public class AdditionRequest
    {
        [Id(0)]
        public int X { get; set; }
        [Id(1)]
        public int Y { get; set; }
    }

    [Schema]
    public class AdditionResponse
    {
        [Id(0)]
        public int Output { get; set; }
    }

    public static class Serializer<T>
    {
        public static byte[] ToBytes(T obj)
        {
            var buffer = new OutputBuffer();
            var writer = new FastBinaryWriter<OutputBuffer>(buffer);
            Serialize.To(writer, obj);
            var output = new byte[buffer.Data.Count];
            Array.Copy(buffer.Data.Array, 0, output, 0, (int)buffer.Position);
            return output;
        }

        public static T FromBytes(byte[] bytes)
        {
            var buffer = new InputBuffer(bytes);
            var data = Deserialize<T>.From(new FastBinaryReader<InputBuffer>(buffer));
            return data;
        }
    }

    public class Descriptors
    {
        public static Method<AdditionRequest, AdditionResponse> Method =
                new Method<AdditionRequest, AdditionResponse>(
                    type: MethodType.DuplexStreaming,
                    serviceName: "AdditonService",
                    name: "AdditionMethod",
                    requestMarshaller: Marshallers.Create(
                        serializer: Serializer<AdditionRequest>.ToBytes,
                        deserializer: Serializer<AdditionRequest>.FromBytes),
                    responseMarshaller: Marshallers.Create(
                        serializer: Serializer<AdditionResponse>.ToBytes,
                        deserializer: Serializer<AdditionResponse>.FromBytes));
    }

    class Program
    {
        static void Main(string[] args)
        {
            RunAsync().Wait();
        }

        private static async Task RunAsync()
        {
            var server = new Grpc.Core.Server
            {
                Ports = { { "127.0.0.1", 5000, ServerCredentials.Insecure } },
                Services = {
                ServerServiceDefinition.CreateBuilder().AddMethod(Descriptors.Method, async (requestStream, responseStream, context) => {
                        await requestStream.ForEachAsync(async additionRequest =>
                        {
                            Console.WriteLine($"Recieved addition request, number1 = {additionRequest.X} --- number2 = {additionRequest.Y}");
                            await responseStream.WriteAsync(new AdditionResponse {Output = additionRequest.X + additionRequest.Y});
                        });
                    })
                    .Build()
            }
            };

            server.Start();

            Console.WriteLine($"Server started under [127.0.0.1:5000]. Press Enter to stop it...");
            Console.ReadLine();

            await server.ShutdownAsync();
        }
    }
}
