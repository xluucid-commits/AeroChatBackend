using System.Net.WebSockets;
using System.Text;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseWebSockets();

var rooms = new ConcurrentDictionary<string, List<WebSocket>>();

app.Map("/chat", async context =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var room = context.Request.Query["room"].ToString();
        if (string.IsNullOrEmpty(room)) room = "Allgemeiner Chat"; 

        using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        
        rooms.AddOrUpdate(room, 
            new List<WebSocket> { webSocket }, 
            (key, list) => { lock (list) { list.Add(webSocket); } return list; });

        Console.WriteLine($"Ein User ist dem Raum '{room}' beigetreten.");
        
        var buffer = new byte[1024 * 4];
        
        try 
        {
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            
            while (!result.CloseStatus.HasValue)
            {
                var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                Console.WriteLine($"[{room}] Nachricht empfangen: {message}");
                
                var messageBytes = Encoding.UTF8.GetBytes(message);
                
                WebSocket[] targetClients;
                if (rooms.TryGetValue(room, out var clientList))
                {
                    lock (clientList)
                    {
                        targetClients = clientList.ToArray();
                    }

                    foreach (var client in targetClients)
                    {
                        if (client != webSocket && client.State == WebSocketState.Open)
                        {
                            await client.SendAsync(new ArraySegment<byte>(messageBytes, 0, messageBytes.Length), result.MessageType, result.EndOfMessage, CancellationToken.None);
                        }
                    }
                }
                
                result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            }
        }
        catch (Exception)
        {
            Console.WriteLine($"Verbindung in Raum '{room}' getrennt.");
        }
        finally 
        {
            if (rooms.TryGetValue(room, out var clientList))
            {
                lock (clientList)
                {
                    clientList.Remove(webSocket);
                }
            }
            Console.WriteLine($"Ein User hat den Raum '{room}' verlassen.");
        }
    }
    else
    {
        context.Response.StatusCode = 400;
    }
});

app.Run();