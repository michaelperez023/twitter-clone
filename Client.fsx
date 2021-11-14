#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"
open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

// Input from Command Line
let myIP = fsi.CommandLineArgs.[1] |> string
let port = fsi.CommandLineArgs.[2] |> string
let id = fsi.CommandLineArgs.[3] |> string
let users = fsi.CommandLineArgs.[4] |> string
let noofclients = fsi.CommandLineArgs.[5] |> string
let serverip = fsi.CommandLineArgs.[6] |> string

let configuration = 
    ConfigurationFactory.ParseString(
        sprintf @"akka {            
            stdout-loglevel : DEBUG
            loglevel : ERROR
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            }
            remote.helios.tcp {
                transport-protocol = tcp
                port = %s
                hostname = %s
            }
    }" port myIP)

let system = ActorSystem.Create("Client", configuration)
type BossMessages = 
    | Start of (int*int*int*string)

let ClientActor (mailbox:Actor<_>) = 
    let mutable id = ""
    let mutable nusers = 0
    let mutable nclients = 0
    let mutable port = ""
    let mutable clientslist = []
    let mutable intervalmap = Map.empty
    let mutable usersList = []
    let mutable subsrank = Map.empty
    let server = system.ActorSelection(sprintf "akka.tcp://Server@%s:8776/user/ServerActor" serverip)

    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive() 
        let (mtype,_,_,_,_) : Tuple<string,string,string,string,string> = downcast message
        match mtype with
        | "Start" ->
            let timestamp = DateTime.Now
            let (_, i, u, n, p) : Tuple<string,string,string,string,string> = downcast message 
            //printerRef <! 
            printf "Client %s Start!!" id
            id <- i
            nusers <- u |> int32
            nclients <- n |> int32
            port <- p
            let mutable usersarr = [| 1 .. nusers |]
            printfn "usersarr=%A" usersarr
            let rand = Random()
            let swap (a: _[]) x y =
                let tmp = a.[x]
                a.[x] <- a.[y]
                a.[y] <- tmp
            let shuffle a =
                Array.iteri (fun i _ -> swap a i (rand.Next(i, Array.length a))) a
            
            shuffle usersarr
            usersList <- usersarr |> Array.toList
            printfn "second userarr=%A" usersarr
            for i in [1 .. nusers] do
                let userkey = usersarr.[i-1] |> string
                subsrank <- Map.add (sprintf "%s_%s" id userkey) ((nusers-1)/i) subsrank
                intervalmap <- Map.add (sprintf "%s_%s" id userkey) i intervalmap

            server <! ("ClientRegister",id,myIP,port,timestamp)
            for i in [1 .. nclients] do
                let istr = i |> string
                clientslist <- istr :: clientslist

        | _ ->
            ignore()
        return! loop()
    }
    loop()

// Start of the algorithm - spawn Boss, the delgator
let boss = spawn system "AdminActor" ClientActor
boss <! ("Start", id, users, noofclients, port)
system.WhenTerminated.Wait()