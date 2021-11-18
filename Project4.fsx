#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

type ServerStart  = {
    messageName: string;
    timeStamp: DateTime;
}

type ClientStart  = {
    messageName: string;
    clientID: string;
    users: int;
    clients: int;
    port: string;
}

type ClientRegistration = {
    messageName: string;
    clientID: string;
    clientIP: string;
    port: string;
    timeStamp: DateTime;
}

type AckClientReg = {
    messageName: string;
    message: string
}

type RegisterUser = {
    messageName: string;
    nextID: int
}

type UserRegistration = {
    messageName: string;
    clientID: string;
    userID: string;
    followers: string;
    timeStamp: DateTime;
}

type AckUserReg = {
    messageName: string;
    userID: string;
    message: string;
}

type Offline = {
    messageName: string;
}

type UserActorMessage =
    | StartTweet
    | StartOtherAction
    | Ready of string * list<string> * ActorSelection * int * string * list<string> * int

type Tweet = {
    messageName: string;
    clientID: string;
    userID: string;
    tweet: string;
    time: DateTime;
}

type RegisterUserWithMentionsActor = {
    messageName: string;
    clientID: string;
    userID: string;
}

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

    let TweetActor (mailbox:Actor<_>) = 
        let mutable tweetCount = 0
        let mutable userTweetsMap = Map.empty

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? Tweet as msg ->
                tweetCount <- tweetCount + 1
                printfn "Tweet: %s" msg.tweet
                let mutable twCount = 0
                //cprinters.[cid] <! sprintf "[%s][TWEET] %s" (timestamp.ToString()) twt

                if userTweetsMap.ContainsKey msg.userID then 
                    twCount <- userTweetsMap.[msg.userID] + 1
                userTweetsMap <- Map.add msg.userID twCount userTweetsMap

                // TODO - implement update feeds
                //usersActor <! UpdateFeeds(cid,uid,twt, "tweeted", DateTime.Now)
                //twTotalTime <- twTotalTime + (timestamp.Subtract reqTime).TotalMilliseconds
                //let averageTime = twTotalTime / tweetCount
                //boss <! ("ServiceStats","","Tweet",(averageTime |> string),DateTime.Now) 
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let MentionsActor (mailbox:Actor<_>) = 
        let mutable usersSet = Set.empty
        let mutable userMentionsMap = Map.empty

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? RegisterUserWithMentionsActor as msg ->
                usersSet <- Set.add msg.userID usersSet
                userMentionsMap <- Map.add msg.userID List.empty userMentionsMap
            | :? Tweet as msg ->
                let tweetSplit = msg.tweet.Split ' '
                for word in tweetSplit do
                    if word.[0] = '@' then
                        let mention = word.[1..(word.Length-1)]
                        if usersSet.Contains mention then
                            let mutable mentionsList = userMentionsMap.[word.[1..(word.Length-1)]]
                            mentionsList <- msg.tweet :: mentionsList
                            userMentionsMap <- Map.add mention mentionsList userMentionsMap
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let HashtagsActor (mailbox:Actor<_>) = 
        let mutable hashtagTweetsMap = Map.empty

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? Tweet as msg ->
                let tweetSplit = msg.tweet.Split ' '
                for word in tweetSplit do
                    if word.[0] = '#' then
                        let hashtag = word.[1..(word.Length-1)]
                        if not (hashtagTweetsMap.ContainsKey hashtag) then
                            hashtagTweetsMap <- Map.add hashtag List.empty hashtagTweetsMap

                        let mutable tweetsList = hashtagTweetsMap.[hashtag]
                        tweetsList <- msg.tweet :: tweetsList
                        hashtagTweetsMap <- Map.add hashtag tweetsList hashtagTweetsMap
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let ServerActor (mailbox:Actor<_>) = 
        //let mutable clientPrinters = Map.empty
        let mutable requestsCount = 0UL
        let mutable tweetsActor = null
        let mutable mentionsActor = null
        let mutable hashtagsActor = null

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let timestamp = DateTime.Now
            match message with
            | :? ServerStart as msg ->
                printfn "Start"
                tweetsActor <- spawn system ("TweetActor") TweetActor
                mentionsActor <- spawn system ("MentionsActor") MentionsActor
                hashtagsActor <- spawn system ("HashtagsActor") HashtagsActor
            | :? ClientRegistration as msg ->
                printfn "Client registering..."
                requestsCount <- requestsCount + 1UL
                //let clientPrinter = system.ActorSelection(sprintf "akka.tcp://TwitterClient@%s:%s/user/Printer" msg.IP msg.port)
                //clientPrinters <- Map.add msg.ID clientPrinter clientPrinters
                //sendToAllActors clientprinters
                let message = "[" + timestamp.ToString() + "][CLIENT_REGISTER] Client " + msg.clientID + " registered with server"
                mailbox.Sender() <! {messageName="AckClientReg"; message=message}
            | :? UserRegistration as msg ->
                //let (_,cid,userid,subscount,reqTime) : Tuple<string,string,string,string,DateTime> = downcast message 
                //usersactor <! Register(cid, userid, subscount,reqTime)
                mentionsActor <! {messageName="RegisterUserWithMentionsActor"; clientID=msg.clientID; userID=msg.userID}
                requestsCount <- requestsCount + 1UL
                let message = "[" + timestamp.ToString() + "][USER_REGISTER] User " + msg.userID + " registered with server"
                mailbox.Sender() <! {messageName="AckUserReg"; userID=msg.userID; message=message}
            | :? Tweet as msg ->
                requestsCount <- requestsCount + 1UL
                printfn "tweeting: %s" msg.tweet
                mentionsActor <! msg // forward tweet to mentions Actor
                hashtagsActor <! msg // forward tweet to hashtags Actor
                tweetsActor <! msg // forward tweet to tweets Actor
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
            let! message = mailbox.Receive() 
            match message with
            | Ready(userID', clientsList', server', usersCount', clientID', hashtagsList', interval') ->
                userID <- userID'
                clientsList <- clientsList'
                server <- server'
                usersCount <- usersCount'
                clientID <- clientID'
                hashtagsList <- hashtagsList'
                interval <- interval' |> double
                online <- true
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, StartTweet)
                //system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(49.0), mailbox.Self, StartOtherAction)
            | StartTweet ->
                if online then
                    let tweetTypes =  [1..4] // change to 5 after retweeting is implemented
                    let tweetType = tweetTypes.[Random().Next(tweetTypes.Length)]
                    let mutable tweet = ""
                    match tweetType with   
                    | 1 ->  // tweet without mention or hashtag
                        tweetCount <- tweetCount + 1
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount)
                    | 2 -> // tweet with mention
                        tweetCount <- tweetCount + 1
                        let mutable mUser = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclid = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randclid +  "_" + mUser
                        while mentionsUser = userID do 
                            mUser <- [1..usersCount].[Random().Next(usersCount)] |> string
                            mentionsUser <- randclid +  "_" + mUser
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with mention @" + mentionsUser
                    | 3 -> // tweet with hashtag
                        tweetCount <- tweetCount + 1
                        let hashtag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with hashtag #" + hashtag
                    | 4 -> // tweet with mention and hashtag
                        tweetCount <- tweetCount + 1
                        let mutable mUser = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclid = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randclid +  "_" + mUser
                        while mentionsUser = userID do 
                            mUser <- [1..usersCount].[Random().Next(usersCount)] |> string
                            mentionsUser <- randclid +  "_" + mUser
                        let hashtag = hashtagsList.[Random().Next(hashtagsList.Length)]                    
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with hashtag #" + hashtag + " and mentioned @" + mentionsUser
                    | 5 -> // retweet
                        printfn "retweeting..."
                        //TODO
                    | _ -> 
                        ()
                    server <! {messageName="Tweet"; clientID=clientID; userID=userID; tweet=tweet; time=DateTime.Now;}
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(interval), mailbox.Self, {messageName="StartTweet";})  
            | StartOtherAction ->
                if online then
                    let actionTypes =  [1..5]
                    let actionType = actionTypes.[Random().Next(actionTypes.Length)]
                    match actionType with   
                    | 1 ->  // follow
                        //TODO
                        printfn "following"
                    | 2 ->  // unfollow
                        //TODO
                        printfn "unfollowing"
                    | 3 ->  // query tweets
                        printfn "querying tweets"
                        //TODO
                    | 4 ->  // query hashtags
                        printfn "querying hashtags"
                        //TODO
                    | 5 ->  // query mentions
                        printfn "querying mentions"
                        //TODO
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, StartOtherAction)
            return! loop()
        }
        loop()

    let ClientActor (mailbox:Actor<_>) = 
        let mutable clientID = ""
        let mutable usersCount = 0
        let mutable clientsList = []
        let mutable usersList = []
        let mutable registeredUsersList = []
        let mutable userIntervalMap = Map.empty
        let mutable userFollowersRankMap = Map.empty
        let mutable userAddress = Map.empty
        let server = system.ActorSelection("akka.tcp://Server@" + serverip + ":8776/user/ServerActor")

        let hashtagsList = ["VenmoItForward"; "SEIZED"; "WWE2K22"; "JusticeForJulius"; "AhmaudArbery"; 
        "NASB"; "Ticketmaster"; "gowon"; "Stacy"; "Garnet"; "Gaetz"; "Accused"; "Omarova"; "Cenk"; "McMuffin";]

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? ClientStart as msg ->
                usersCount <- msg.users
                clientID <- msg.clientID
                printerRef <! "Client " + clientID + " Start!"
                printerRef <! "Number of users: " + string(usersCount)

                let mutable usersArray= [| 1 .. msg.users |]
                //printerRef <! "first users array=%s" usersarr
                let swap (a: _[]) x y =
                    let tmp = a.[x]
                    a.[x] <- a.[y]
                    a.[y] <- tmp
                let shuffle a =
                    Array.iteri (fun i _ -> swap a i (Random().Next(i, Array.length a))) a
                
                shuffle usersArray
                usersList <- usersArray |> Array.toList
                //printerRef <! "second users array==%s" usersarr
                for i in [1 .. (msg.users |> int32)] do
                    let userKey = usersArray.[i-1] |> string
                    userFollowersRankMap <- Map.add (clientID + "_" + userKey) ((msg.users-1)/i) userFollowersRankMap
                    userIntervalMap <- Map.add (clientID  + "_" + userKey) i userIntervalMap
                
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
                let followers = userFollowersRankMap.[userID] |> string
                server <! {messageName="UserRegistration"; clientID=clientID; userID=userID; followers=followers; timeStamp=DateTime.Now}
                registeredUsersList <- userID :: registeredUsersList
                //printerRef <! "Users registered: " + string(msg.nextID)
                
                if msg.nextID < usersCount then
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, {messageName="RegisterUser"; nextID=msg.nextID + 1;})
            | :? AckUserReg as msg ->
                printerRef <! msg.message
                let mutable baseInterval = usersCount/100
                if baseInterval < 5 then
                    baseInterval <- 5
                userAddress.[msg.userID] <! Ready(msg.userID, clientsList, server, usersCount, clientID, hashtagsList, (baseInterval*userIntervalMap.[msg.userID]))
                //{messageName="Ready"; userID=msg.userID; clientsList=clientsList; server=server; usersCount=usersCount; clientID=clientID; hashtagsList=hashtagsList; interval=(baseInterval*intervalMap.[msg.userID])}
            | :? Offline as msg ->
                printerRef <! "Going offline"
                //TODO
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    // Start - spawn boss
    let boss = spawn system "AdminActor" ClientActor

    boss <! {messageName="ClientStart"; clientID=ID; users=(usersCount |> int32); clients=(clientsCount |> int32); port=port}

    system.WhenTerminated.Wait()