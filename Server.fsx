#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"

#load @"./Messages.fsx"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

open Messages

let serverIP = fsi.CommandLineArgs.[1] |> string

let configuration = ConfigurationFactory.ParseString(
                        @"akka {            
                            actor {
                                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                            }
                            remote.helios.tcp {
                                transport-protocol = tcp
                                port = 8776
                                hostname = " + serverIP + "
                            }
                        }")

let system = ActorSystem.Create("Server", configuration)

let ServerActor (mailbox:Actor<_>) = 
    //let mutable clientPrinters = Map.empty
    let mutable requests = 0UL

    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive()
        let timestamp = DateTime.Now
        match message with
        | :? ServerStart as msg ->
            printfn "Start"
        | :? ClientRegistration as msg -> 
            printfn "Client registering..."
            requests <- requests + 1UL
            //let clientPrinter = system.ActorSelection(sprintf "akka.tcp://TwitterClient@%s:%s/user/Printer" msg.IP msg.port)
            //clientPrinters <- Map.add msg.ID clientPrinter clientPrinters
            //sendToAllActors clientprinters
            let message = "[" + timestamp.ToString() + "][CLIENT_REGISTER] Client " + msg.clientID + " registered with server"
            mailbox.Sender() <! {messageName="AckClientReg"; message=message}
        | :? UserRegistration as msg ->
            //let (_,cid,userid,subscount,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message 
            //usersactor <! Register(cid, userid, subscount,reqTime)
            //mentionsactor <! MentionsRegister(cid, userid)
            requests <- requests + 1UL
            let message = "[" + timestamp.ToString() + "][USER_REGISTER] User " + msg.userID + " registered with server"
            mailbox.Sender() <! {messageName="AckUserReg"; userID=msg.userID; message=message}
        | :? Tweet as msg ->
            requests <- requests + 1UL
            printfn "tweeting: %s" msg.tweet
            //send tweet to mentions actor
        | _ ->
            ignore()
        return! loop()
    }
    loop()

// Start - spawn boss
let boss = spawn system "ServerActor" ServerActor

boss <! {messageName="ServerStart"; timeStamp=DateTime.Now}

system.WhenTerminated.Wait()