#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

//let serverIP = fsi.CommandLineArgs.[1] |> string

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {            
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = 8776
                hostname = localhost
            }
        }")

let system = ActorSystem.Create("Server", configuration)

let ServerActor (mailbox:Actor<_>) = 
    let mutable clientprinters = Map.empty
    let mutable requests = 0UL

    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive()
        let (mtype,_,_,_,_) : Tuple<string,string,string,string,DateTime> = downcast message 
        let timestamp = DateTime.Now
        match mtype with
        | "Start" ->
            printfn "Start!!"      
        | "ClientRegister" -> 
            let (_,cid,cliIP,port,_) : Tuple<string,string,string,string,DateTime> = downcast message 
            requests <- requests + 1UL
            let clientp = system.ActorSelection(sprintf "akka.tcp://TwitterClient@%s:%s/user/Printer" cliIP port)
            clientprinters <- Map.add cid clientp clientprinters
            //sendToAllActors clientprinters
            mailbox.Sender() <! ("AckClientReg",sprintf "[%s][CLIENT_REGISTER] Client %s registered with server" (timestamp.ToString()) cid,"","","")
        | _ ->
            ignore()
        return! loop()
    }
    loop()

// Start - spawn boss
let boss = spawn system "ServerActor" ServerActor

boss <! ("Start","","","",DateTime.Now)
system.WhenTerminated.Wait()