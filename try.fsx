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

if "server" = (fsi.CommandLineArgs.[1] |> string) then
    let serverIP = fsi.CommandLineArgs.[2] |> string

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

if "client" = (fsi.CommandLineArgs.[1] |> string) then
    // Command Line input
    let myIP = fsi.CommandLineArgs.[2] |> string
    let port = fsi.CommandLineArgs.[3] |> string
    let ID = fsi.CommandLineArgs.[4] |> string
    let usersCount = fsi.CommandLineArgs.[5] |> string
    let clientsCount = fsi.CommandLineArgs.[6] |> string
    let serverip = fsi.CommandLineArgs.[7] |> string

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
        let mutable online = false
        let mutable tweetCount = 0
        let mutable userID = ""
        let mutable clientsList = []
        let mutable server = ActorSelection()
        let mutable usersCount = 0
        let mutable clientID = ""
        let mutable hashtagsList = []
        let mutable interval = 0.0

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? Ready as msg ->
                printerRef <! "Ready!"
                userID <- msg.userID
                clientsList <- msg.clientsList
                server <- msg.server
                usersCount <- msg.usersCount
                clientID <- msg.clientID
                hashtagsList <- msg.hashtagsList
                interval <- msg.time |> double
                online <- true
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(49.0), mailbox.Self, {messageName="StartTweet";})
            | :? StartTweet as msg ->
                printerRef <! "Starting Tweeting!"
                if online then
                    tweetCount <- tweetCount + 1
                    let tweet = userID + " tweeted -> tweet_" + string(tweetCount)
                    //server <! ("Tweet", clientID, userID, tweetMsg, DateTime.Now)
                    server <! {messageName="Tweet"; clientID=clientID; userID=userID; tweet=tweet; time=DateTime.Now;}
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let ClientActor (mailbox:Actor<_>) = 
        let mutable clientID = ""
        let mutable usersCount = 0
        let mutable clientsList = []
        let mutable usersList = []
        let mutable registeredUsersList = []
        let mutable intervalMap = Map.empty
        let mutable subsrank = Map.empty
        let mutable userAddress = Map.empty
        let server = system.ActorSelection("akka.tcp://Server@" + serverip + ":8776/user/ServerActor")

        // New vars, may need to send to own actor
        let mutable tweetCounter = 0

        let hashtagsList = ["hashtag1"; "hashtag2"]

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? ClientStart as msg ->
                usersCount <- msg.users
                clientID <- msg.clientID
                printerRef <! "Client " + clientID + " Start!"
                printerRef <! "Number of users: " + string(usersCount)

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
                
                server <! {messageName="ClientRegistration"; clientID=clientID; clientIP=myIP; port=msg.port; timeStamp=DateTime.Now}
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
                
                if msg.nextID < usersCount then
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, {messageName="RegisterUser"; nextID=msg.nextID + 1;})
            | :? AckUserReg as msg ->
                printerRef <! msg.message
                let mutable baseInterval = usersCount/100
                if baseInterval < 5 then
                    baseInterval <- 5
                userAddress.[msg.userID] <! {messageName="Ready"; userID=msg.userID; clientsList=clientsList; server=server; usersCount=usersCount; clientID=clientID; hashtagsList=hashtagsList; time=(baseInterval*intervalMap.[msg.userID])}
            | :? Offline as msg ->
                printerRef <! "Going offline"
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    // Start - spawn boss
    let boss = spawn system "AdminActor" ClientActor

    boss <! {messageName="ClientStart"; clientID=ID; users=(usersCount |> int32); clients=(clientsCount |> int32); port=port}

    system.WhenTerminated.Wait()