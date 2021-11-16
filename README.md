# DOS-Project4 - Twitter Clone
Team Members: Blas Kojusner and Michael Perez

COP5615 - Dr. Alin Dobra

November 29 2021

1. Open two or more terminals on the same number of machines, then navigate to the directory /DOS-Project4/ in each terminal. 
2. Run the command "dotnet fsi Server.fsx server_IP_address" on the server.
3. Run the command "dotnet fsi Client.fsx client_IP_address port client_ID number_of_users number_of_clients server_IP_address" on each client.

For example, to simulate 1000 users running on three clients, run the commands:
  dotnet fsi Client.fsx client_1_IP_address port 1 1000 3 server_IP_address
  dotnet fsi Client.fsx client_2_IP_address port 2 1000 3 server_IP_address
  dotnet fsi Client.fsx client_3_IP_address port 3 1000 3 server_IP_address
