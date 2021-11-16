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

// Command Line input
let myIP = fsi.CommandLineArgs.[1] |> string
let port = fsi.CommandLineArgs.[2] |> string
let ID = fsi.CommandLineArgs.[3] |> string
let users = fsi.CommandLineArgs.[4] |> string
let noofclients = fsi.CommandLineArgs.[5] |> string
let serverip = fsi.CommandLineArgs.[6] |> string

let configuration = ConfigurationFactory.ParseString(
                        @"akka {            
                            actor {
                                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                            }
                            remote.helios.tcp {
                                transport-protocol = tcp
                                port = " + port + "
                                hostname = " + myIP + "
                            }
                    }")
let system = ActorSystem.Create("Client", configuration)

// Printer Actor - to print the output
let printerActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let! message = mailbox.Receive()
        printfn "%s" message
        return! loop()
    }
    loop()
let printerRef = spawn system "Printer" printerActor

let UserActor (mailbox:Actor<_>) = 
    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive() 
        match message with
        | :? Ready as msg ->
            printerRef <! "Ready!"
        | _ ->
            ignore()
        return! loop()
    }
    loop()

let ClientActor (mailbox:Actor<_>) = 
    let mutable clientID = ""
    let mutable users = 0
    let mutable clientsList = []
    let mutable usersList = []
    let mutable registeredUsersList = []
    let mutable intervalMap = Map.empty
    let mutable subsrank = Map.empty
    let mutable userAddress = Map.empty
    let server = system.ActorSelection("akka.tcp://Server@" + serverip + ":8776/user/ServerActor")

    let hashtagsList = ["hashtag1"; "hashtag2"]

    let rec loop () = actor {
        let! (message:obj) = mailbox.Receive() 
        match message with
        | :? ClientStart as msg ->
            users <- msg.users
            clientID <- msg.clientID
            printerRef <! "Client " + clientID + " Start!"
            printerRef <! "Number of users: " + string(users)

            let mutable usersarr = [| 1 .. msg.users |]
            //printerRef <! "usersarr=%A" usersarr
            let swap (a: _[]) x y =
                let tmp = a.[x]
                a.[x] <- a.[y]
                a.[y] <- tmp
            let shuffle a =
                Array.iteri (fun i _ -> swap a i (Random().Next(i, Array.length a))) a
            
            shuffle usersarr
            usersList <- usersarr |> Array.toList
            //printerRef <! "second userarr=%A" usersarr
            for i in [1 .. (msg.users |> int32)] do
                let userkey = usersarr.[i-1] |> string
                subsrank <- Map.add (clientID + "_" + userkey) ((msg.users-1)/i) subsrank
                intervalMap <- Map.add (clientID  + "_" + userkey) i intervalMap
            
            server <! {messageName="ClientRegistration"; ID=clientID; IP=myIP; port=msg.port; timeStamp=DateTime.Now}
            for i in [1 .. msg.clients] do
                clientsList <- (i |> string) :: clientsList
        | :? AckClientReg as msg ->
            printerRef <! msg.message
            mailbox.Self <! {messageName="RegisterUser"; nextID=1}
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, {messageName="Offline";})
        | :? RegisterUser as msg ->
            let mutable userID = clientID  + "_" + (usersList.[msg.nextID-1] |> string)
            let userRef = spawn system ("User_" + userID) UserActor
            userAddress <- Map.add userID userRef userAddress
            let subsstr = subsrank.[userID] |> string
            server <! {messageName="UserRegistration"; clientID=clientID; userID=userID; subsstr=subsstr; timeStamp=DateTime.Now}
            registeredUsersList <- userID :: registeredUsersList

            //printerRef <! "Users registered: " + string(msg.nextID)
            
            if msg.nextID < users then
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, {messageName="RegisterUser"; nextID=msg.nextID + 1;})
        | :? AckUserReg as msg ->
            printerRef <! msg.message
            let mutable baseInterval = users/100
            if baseInterval < 5 then
                baseInterval <- 5
            userAddress.[msg.userID] <! {messageName="Ready"; userID=msg.userID; clientsList=clientsList; server=server; users=users; clientID=clientID; hashtagsList=hashtagsList; time=(baseInterval*intervalMap.[msg.userID])}
        | :? Offline as msg ->
            printerRef <! "Going offline"
        | _ ->
            ignore()
        return! loop()
    }
    loop()

// Start - spawn boss
let boss = spawn system "AdminActor" ClientActor

boss <! {messageName="ClientStart"; clientID=ID; users=(users |> int32); clients=(noofclients |> int32); port=port}

system.WhenTerminated.Wait()