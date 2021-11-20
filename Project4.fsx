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
    followersCount: string;
    timeStamp: DateTime;
}

type AckUserReg = {
    messageName: string;
    userID: string;
    message: string;
}

type Offline = {
    messageName: string;
    //userID: string;
    //timeStamp: DateTime;
}

type UserActorMessage =
    | StartTweet
    | StartOtherAction
    | Ready of string * list<string> * ActorSelection * int * string * list<string> * int
    | GoOffline

type Tweet = {
    messageName: string;
    clientID: string;
    userID: string;
    tweet: string;
    time: DateTime;
}

type Retweet = {
    messageName: string;
    clientID: string;
    userID: string;
    time: DateTime;
}

type RegisterUserWithMentionsActor = {
    messageName: string;
    clientID: string;
    userID: string;
}

type RegisterUserWithUsersActor = {
    messageName: string;
    clientID: string;
    userID: string;
    followersCount: string;
    time: DateTime;
}

type Follow = {
    messageName: string;
    clientID: string;
    userID: string;
    userToFollowID: string;
    time: DateTime;
}

type UpdateFeeds = {
    messageName: string;
    clientID: string;
    userID: string;
    tweet: string;
    actionType: string;
    time: DateTime;
}

type InitTweetActor = {
    messageName: string;
    actor: IActorRef;
}

type UpdateFeedTable = {
    messageName: string;
    followerID: string;
    userID: string;
    tweet: string;
}

type InitUsersActor = {
    messageName: string;
    actor: IActorRef;
    actor1: IActorRef;
}

type UpdateRetweetFeedTable = {
    messageName: string;
    id: string;
    userID: string;
    tweet: string;
}

type InitRetweetsActor = {
    messageName: string;
    actor: IActorRef;
    actor1: IActorRef;
}

type GoOffline = {
    messageName: string;
    clientID: string;
    userID: string;
    time: DateTime;
}

type GoOnline = {
    messageName: string;
    clientID: string;
    userID: string;
    cAdmin: IActorRef;
    time: DateTime;
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

    let UsersActor (mailbox:Actor<_>) = 
        let mutable userFollowersCountMap = Map.empty
        let mutable userUsersFollowingSetMap = Map.empty
        let mutable followTime = 0.0
        let mutable userServiceCount = 0
        let mutable showfeedactor = mailbox.Self
        let mutable retweetactor = mailbox.Self
        let mutable offLineUsers = Set.empty

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let timeStamp = DateTime.Now
            match message with
            | :? InitUsersActor as msg ->
                showfeedactor <- msg.actor
                retweetactor <- msg.actor1
            | :? RegisterUserWithUsersActor as msg ->
                userFollowersCountMap <- Map.add msg.userID (msg.followersCount |> int) userFollowersCountMap
                userUsersFollowingSetMap <- Map.add msg.userID Set.empty userUsersFollowingSetMap
                followTime <- followTime + (timeStamp.Subtract msg.time).TotalMilliseconds
                userServiceCount <- userServiceCount + 1
            | :? Follow as msg ->
                userServiceCount <- userServiceCount + 1
                if userUsersFollowingSetMap.ContainsKey msg.userToFollowID && not (userUsersFollowingSetMap.[msg.userToFollowID].Contains msg.userToFollowID) && userUsersFollowingSetMap.[msg.userToFollowID].Count < userFollowersCountMap.[msg.userToFollowID] then
                    let mutable userUsersFollowingSet = userUsersFollowingSetMap.[msg.userToFollowID]
                    userUsersFollowingSet <- Set.add msg.userID userUsersFollowingSet
                    userUsersFollowingSetMap <- Map.add msg.userID userUsersFollowingSet userUsersFollowingSetMap
                    let message = "[" + timeStamp.ToString() + "][FOLLOW] User " + msg.clientID + " started following " + msg.userToFollowID
                    printfn $"{message}"
                    //cprinters.[cid] <! sprintf "[%s][FOLLOW] User %s started following %s" (timestamp.ToString()) uid fid
                followTime <- followTime + (timeStamp.Subtract msg.time).TotalMilliseconds
            | :? UpdateFeeds as msg ->
                userServiceCount <- userServiceCount + 1
                for id in userUsersFollowingSetMap.[msg.userID] do // This for loop does not get accesses??
                    if true then // Just temp if statement to leave indentations right
                        showfeedactor <! {messageName="UpdateFeedTable"; followerID=msg.clientID; userID=msg.userID; tweet=msg.tweet;} // FollowerID is just id
                        retweetactor <! {messageName="UpdateRetweetFeedTable"; id=msg.clientID; userID=msg.userID; tweet=msg.tweet} // clientID is just id
                        if not (offLineUsers.Contains msg.clientID) then
                            let splits = id.Split '_'
                            let sendtoid = splits.[0]
                            if msg.actionType = "tweeted" then
                                //cprinters.[sendtoid] <! s
                                printf "[%s][NEW_FEED] For User: %s -> %s" (msg.time.ToString()) id msg.tweet    
                            else
                                //cprinters.[sendtoid] <! s
                                printf "[%s][NEW_FEED] For User: %s -> %s %s - %s" (msg.time.ToString()) id msg.userID msg.actionType msg.tweet
                followTime <- followTime + (timeStamp.Subtract msg.time).TotalMilliseconds
            | :? GoOffline as msg ->
                userServiceCount <- userServiceCount + 1
                printfn "We are here going offline"
                offLineUsers <- Set.add msg.userID offLineUsers
                followTime <- followTime + (timeStamp.Subtract msg.time).TotalMilliseconds
                //cprinters.[cid] <! sprintf "[%s][OFFLINE] User %s is going offline" (timestamp.ToString()) uid
            | :? GoOnline as msg  ->
                userServiceCount <- userServiceCount + 1
                printfn "We are now going online"
                offLineUsers <- Set.remove msg.userID offLineUsers
                //showfeedactor <! ShowFeeds(cid, uid, cadmin)
                followTime <- followTime + (timeStamp.Subtract msg.time).TotalMilliseconds
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let TweetActor (mailbox:Actor<_>) = 
        let mutable tweetCount = 0
        let mutable userTweetsMap = Map.empty
        let mutable usersActor = mailbox.Self

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? InitTweetActor as msg ->
                usersActor <- msg.actor
            | :? Tweet as msg ->
                tweetCount <- tweetCount + 1
                printfn "Tweet: %s" msg.tweet
                let mutable twCount = 0
                //cprinters.[cid] <! sprintf "[%s][TWEET] %s" (timestamp.ToString()) twt

                if userTweetsMap.ContainsKey msg.userID then 
                    twCount <- userTweetsMap.[msg.userID] + 1
                userTweetsMap <- Map.add msg.userID twCount userTweetsMap

                // Update Feeds
                usersActor <! {messageName="UpdateFeeds"; clientID=msg.clientID; userID=msg.userID; tweet=msg.tweet; actionType = "tweet"; time=DateTime.Now;}
                
                // These are performance metrics
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

    let RetweetsActor (mailbox:Actor<_>) = 
        let mutable userTweetsMap = Map.empty
        let mutable feedtable = Map.empty
        let mutable retweetCount = 0.0
        let mutable usersActor = mailbox.Self
        let mutable tweetActor = mailbox.Self


        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive() 
            match message with
            | :? InitRetweetsActor as msg ->
                usersActor <- msg.actor
                tweetActor <- msg.actor1
            | :? UpdateRetweetFeedTable as msg ->
                let mutable feedTableList = []
                if feedtable.ContainsKey msg.id then
                    feedTableList <- feedtable.[msg.id]
                feedTableList <- msg.tweet :: feedTableList
                feedtable <- Map.remove msg.id feedtable
                feedtable <- Map.add msg.id feedTableList feedtable
            | :? Retweet as msg ->
                printfn "retweeting" // TODO
                if feedtable.ContainsKey msg.userID then
                    printfn "Finally made it inside here" // Working to get this printing
                    retweetCount <- retweetCount + 1.0
                    let randTweet = feedtable.[msg.userID].[Random().Next(feedtable.[msg.userID].Length)]
                    //cprinters.[msg.clientID] <! sprintf "[%s][RE_TWEET] %s retweeted -> %s" (timeStamp.ToString()) msg.userID randTweet
                    //reTweetTime <- reTweetTime + (timeStamp.Subtract msg.time).TotalMilliseconds
                    //let averageTime = reTweetTime / reTweetCount
                    //mailbox.Sender() <! ("ServiceStats","","ReTweet",(averageTime |> string),DateTime.Now)
                    usersActor <! {messageName="UpdateFeeds"; clientID=msg.clientID; userID=msg.userID; tweet=randTweet; actionType="retweeted"; time=DateTime.Now}
                    //tweetActor <! IncTweet(uid) // Increment a user's tweet count on the tweet actor
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let ShowFeedActor (mailbox:Actor<_>) = 
        let mutable clientPrinter = Map.empty
        let mutable feedtable = Map.empty

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let timestamp = DateTime.Now
            match message with
            | :? UpdateFeedTable as msg ->
                let mutable tmpList = []
                if feedtable.ContainsKey msg.followerID then
                    tmpList <- feedtable.[msg.followerID]
                tmpList  <- msg.tweet :: tmpList
                feedtable <- Map.remove msg.followerID feedtable
                feedtable <- Map.add msg.followerID tmpList feedtable
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let ServerActor (mailbox:Actor<_>) = 
        //let mutable clientPrinters = Map.empty
        let mutable requestsCount = 0UL
        let mutable usersActor = null
        let mutable tweetsActor = null
        let mutable mentionsActor = null
        let mutable hashtagsActor = null
        let mutable retweetsActor = null
        let mutable showfeedactor = null

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let timestamp = DateTime.Now
            match message with
            | :? ServerStart as msg ->
                printfn "Start"
                usersActor <- spawn system ("UsersActor") UsersActor
                tweetsActor <- spawn system ("TweetActor") TweetActor
                mentionsActor <- spawn system ("MentionsActor") MentionsActor
                hashtagsActor <- spawn system ("HashtagsActor") HashtagsActor
                retweetsActor <- spawn system ("RetweetsActor") RetweetsActor
                showfeedactor <- spawn system ("ShowFeedActor") ShowFeedActor

                // Send actors needed for reference
                tweetsActor <! {messageName="InitTweetActor"; actor=usersActor;}
                usersActor <! {messageName="InitUsersActor"; actor=showfeedactor; actor1=retweetsActor}
                retweetsActor <! {messageName="InitUsersActor"; actor=usersActor; actor1=tweetsActor}
            | :? ClientRegistration as msg ->
                printfn "Client registering..."
                requestsCount <- requestsCount + 1UL
                //let clientPrinter = system.ActorSelection(sprintf "akka.tcp://TwitterClient@%s:%s/user/Printer" msg.IP msg.port)
                //clientPrinters <- Map.add msg.ID clientPrinter clientPrinters
                //sendToAllActors clientprinters
                let message = "[" + timestamp.ToString() + "][CLIENT_REGISTER] Client " + msg.clientID + " registered with server"
                mailbox.Sender() <! {messageName="AckClientReg"; message=message}
            | :? UserRegistration as msg ->
                usersActor <! {messageName="RegisterUserWithUsersActor"; clientID=msg.clientID; userID=msg.userID; followersCount=msg.followersCount; time=msg.timeStamp}
                mentionsActor <! {messageName="RegisterUserWithMentionsActor"; clientID=msg.clientID; userID=msg.userID}
                requestsCount <- requestsCount + 1UL
                let message = "[" + timestamp.ToString() + "][USER_REGISTER] User " + msg.userID + " registered with server"
                mailbox.Sender() <! {messageName="AckUserReg"; userID=msg.userID; message=message}
            | :? Tweet as msg ->
                requestsCount <- requestsCount + 1UL
                mentionsActor <! msg // forward tweet to mentions Actor
                hashtagsActor <! msg // forward tweet to hashtags Actor
                tweetsActor <! msg // forward tweet to tweets Actor
            | :? Retweet as msg ->
                requestsCount <- requestsCount + 1UL
                retweetsActor <! msg
            | :? Follow as msg ->
                requestsCount <- requestsCount + 1UL
                usersActor <! msg // forward tweet to users Actor
            | :? GoOffline as msg ->
                requestsCount <- requestsCount + 1UL
                usersActor <! msg
            | :? GoOnline as msg ->
                requestsCount <- requestsCount + 1UL
                usersActor <! {messageName=msg.messageName; clientID=msg.clientID; userID=msg.userID; cAdmin=mailbox.Sender(); time=msg.time}
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
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(49.0), mailbox.Self, StartOtherAction)
            | StartTweet ->
                if online then
                    let tweetTypes =  [1..5] // change to 5 after retweeting is implemented
                    let tweetType = tweetTypes.[Random().Next(tweetTypes.Length)]
                    let mutable tweet = ""
                    match tweetType with   
                    | 1 ->  // tweet without mention or hashtag
                        tweetCount <- tweetCount + 1
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount)
                        server <! {messageName="Tweet"; clientID=clientID; userID=userID; tweet=tweet; time=DateTime.Now;}
                    | 2 -> // tweet with mention
                        tweetCount <- tweetCount + 1
                        let mutable mUser = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclid = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randclid +  "_" + mUser
                        while mentionsUser = userID do 
                            mUser <- [1..usersCount].[Random().Next(usersCount)] |> string
                            mentionsUser <- randclid +  "_" + mUser
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with mention @" + mentionsUser
                        server <! {messageName="Tweet"; clientID=clientID; userID=userID; tweet=tweet; time=DateTime.Now;}
                    | 3 -> // tweet with hashtag
                        tweetCount <- tweetCount + 1
                        let hashtag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with hashtag #" + hashtag
                        server <! {messageName="Tweet"; clientID=clientID; userID=userID; tweet=tweet; time=DateTime.Now;}
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
                        server <! {messageName="Tweet"; clientID=clientID; userID=userID; tweet=tweet; time=DateTime.Now;}
                    | 5 -> // retweet
                        server <! {messageName="Retweet"; clientID=clientID; userID=userID; time=DateTime.Now;}
                    | _ -> 
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(interval), mailbox.Self, {messageName="StartTweet";})  
            | StartOtherAction ->
                if online then
                    let actionTypes =  [1]
                    let actionType = actionTypes.[Random().Next(actionTypes.Length)]
                    match actionType with
                    | 1 ->  // follow
                        let mutable userToFollow = [1 .. usersCount].[Random().Next(usersCount)] |> string
                        let mutable randClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable userToFollowID = randClientID + "_" + userToFollow
                        while userToFollowID = userID do 
                            userToFollow <- [1 .. usersCount].[Random().Next(usersCount)] |> string
                            userToFollowID <- randClientID + "_" + userToFollow
                        server <! {messageName="Follow"; clientID=clientID; userID=userID; userToFollowID=userToFollowID; time=DateTime.Now;}
                    | 2 ->  // unfollow
                        //TODO
                        printfn "unfollowing"
                    | 3 ->  // query tweets
                        printfn "querying tweets"
                        //TODO
                    | 4 ->  // query hashtags
                        (*let hashTag = topHashTags.[htagRandReq.Next(topHashTags.Length)]
                        server <! ("QueryHashtags",cliId,myId,hashTag,DateTime.Now)*)
                        printfn "querying hashtags"
                        //TODO
                    | 5 ->  // query mentions
                        printfn "querying mentions"
                        (*let mutable mUser = [1 .. usersCount].[mentionsRandReq.Next(usersCount)] |> string
                        let mutable randclid = clientList.[clientRand.Next(clientList.Length)]
                        let mutable mentionsUser = sprintf "%s_%s" randclid mUser
                        server <! ("QueryMentions",cliId,myId,mentionsUser,DateTime.Now)*)
                        //TODO
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, StartOtherAction)
            | GoOffline ->
                online <- false
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
        let mutable currentlyOffline = Set.empty
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
                server <! {messageName="UserRegistration"; clientID=clientID; userID=userID; followersCount=followers; timeStamp=DateTime.Now}
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
                let timestamp = DateTime.Now
                let userOffline = Random()
                let mutable totalUsers = registeredUsersList.Length
                totalUsers <- (30*totalUsers)/100
                let mutable tempOfflineSet = Set.empty

                for i in [1 .. totalUsers] do
                    let mutable nextOffline = registeredUsersList.[userOffline.Next(registeredUsersList.Length)]
                    
                    while currentlyOffline.Contains(nextOffline) || tempOfflineSet.Contains(nextOffline) do
                        nextOffline <- registeredUsersList.[userOffline.Next(registeredUsersList.Length)]
                    
                    server <! {messageName="GoOffline"; clientID=clientID; userID=nextOffline; time=timestamp}
                    userAddress.[nextOffline] <! GoOffline
                    tempOfflineSet <- Set.add nextOffline tempOfflineSet

                // Doesn't seem to get triggered after looking at the logs
                for goOnline in currentlyOffline do
                    server <! {messageName="GoOnline"; clientID=clientID; userID=goOnline; time=timestamp}
                    
                currentlyOffline <- Set.empty
                currentlyOffline <- tempOfflineSet
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, {messageName="Offline";})

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
