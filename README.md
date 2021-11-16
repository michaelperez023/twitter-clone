# DOS-Project4 - Twitter Clone
Team Members: Blas Kojusner and Michael Perez

COP5615 - Dr. Alin Dobra

November 29 2021

1. Open two or more terminals on the same number of machines, then navigate to the directory /DOS-Project4/ in each terminal. 
2. Run the command "dotnet fsi Server.fsx <server IP address>" on the server.
3. Run the command "dotnet fsi Client.fsx <client IP address> <port> <client_ID>  <number of users> <number of clients> <server IP address>" on each client.

For example, to simulate 1000 users running on three clients, run the commands:
  dotnet fsi Client.fsx <client 1 IP address> <port> 1 1000 3 <server IP address>
  dotnet fsi Client.fsx <client 2 IP address> <port> 2 1000 3 <server IP address>
  dotnet fsi Client.fsx <client 3 IP address> <port> 3 1000 3 <server IP address>
