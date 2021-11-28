#time "on"
#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"
#r "nuget: Akka.TestKit"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

type UserActorMessages = 
    | Ready of (string*list<string>*ActorSelection*int*string*List<string>*int)
    | StartTweet
    | StartOtherAction
    | UserActorGoOffline

type UsersActorMessages = 
    | InitUsersActor of IActorRef * IActorRef
    | RegisterUserWithUsersActor of string * string * DateTime
    | ClientFollow of string * string * string * DateTime
    | UsersActorGoOnline of string * string * IActorRef * DateTime
    | UsersActorGoOffline of string * string * DateTime
    | UpdateFeeds of string * string * DateTime

type RetweetsActorMessages = 
    | InitRetweetsActor of IActorRef * IActorRef
    | UpdateRetweetClientPrinters of Map<string,ActorSelection>
    | UpdateRetweetFeedTable of string * string * string
    | Retweet of string * string * DateTime

type ShowFeedActorMessages = 
    | UpdateFeedTable of string * string * string

type TweetsHashtagsMentionsActorsMessages = 
    | InitTweetsActor of IActorRef
    | Tweet of string * string * string * DateTime
    | UpdateTweetClientPrinters of (Map<string,ActorSelection>)
    | PrintTweetInfo of (Map<string,Set<string>>*Map<string,string>*uint64)
    | IncreaseTweetCount of (string)
    | RegisterUserWithMentionsActor of string * string

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
        let mutable showfeedActor = null
        let mutable retweetActor = null
        let mutable offlineUserIDsSet = Set.empty

        let rec loop () = actor {
            let! message = mailbox.Receive()
            let timeStamp = DateTime.Now
            match message with
            | InitUsersActor (actor1, actor2) ->
                showfeedActor <- actor1
                retweetActor <- actor2
            | RegisterUserWithUsersActor (userID', followersCount', time') ->
                userFollowersCountMap <- Map.add userID' (followersCount' |> int) userFollowersCountMap
                userUsersFollowingSetMap <- Map.add userID' Set.empty userUsersFollowingSetMap
                followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
                userServiceCount <- userServiceCount + 1
            | ClientFollow (clientID', userID', userToFollowID', time') ->
                userServiceCount <- userServiceCount + 1
                if userUsersFollowingSetMap.ContainsKey userToFollowID' && not (userUsersFollowingSetMap.[userToFollowID'].Contains userToFollowID') && userUsersFollowingSetMap.[userToFollowID'].Count < userFollowersCountMap.[userToFollowID'] then
                    let mutable userUsersFollowingSet = userUsersFollowingSetMap.[userToFollowID']
                    userUsersFollowingSet <- Set.add userID' userUsersFollowingSet
                    userUsersFollowingSetMap <- Map.add userID' userUsersFollowingSet userUsersFollowingSetMap
                    let message = "[" + timeStamp.ToString() + "][FOLLOW] User " + clientID' + " started following " + userToFollowID'
                    printfn $"{message}"
                    //cprinters.[cid] <! sprintf "[%s][FOLLOW] User %s started following %s" (timestamp.ToString()) uid fid
                followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
            | UpdateFeeds (clientID', userID', time') -> //TODO
                printfn "HERE UPDATING FEEDS"
                (*userServiceCount <- userServiceCount + 1
                for id in userUsersFollowingSetMap.[msg.userID] do // This for loop does not get accesses??
                    if true then // Just temp if statement to leave indentations right
                        showfeedActor <! {messageName="UpdateFeedTable"; followerID=msg.clientID; userID=msg.userID; tweet=msg.tweet;} // FollowerID is just id
                        retweetActor <! {messageName="UpdateRetweetFeedTable"; id=msg.clientID; userID=msg.userID; tweet=msg.tweet} // clientID is just id
                        if not (offlineUserIDsSet.Contains msg.clientID) then
                            let splits = id.Split '_'
                            let sendtoid = splits.[0]
                            if msg.actionType = "tweeted" then
                                //cprinters.[sendtoid] <! s
                                printf "[%s][NEW_FEED] For User: %s -> %s" (msg.time.ToString()) id msg.tweet    
                            else
                                //cprinters.[sendtoid] <! s
                                printf "[%s][NEW_FEED] For User: %s -> %s %s - %s" (msg.time.ToString()) id msg.userID msg.actionType msg.tweet
                followTime <- followTime + (timeStamp.Subtract msg.time).TotalMilliseconds*)
            | UsersActorGoOnline (clientID', userID', cAdmin', time') ->
                    userServiceCount <- userServiceCount + 1
                    printfn "%s going online" userID'
                    offlineUserIDsSet <- Set.remove userID' offlineUserIDsSet
                    //showfeedactor <! ShowFeeds(cid, uid, cadmin) // TODO
                    followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
            | UsersActorGoOffline (clientID', userID', time') ->
                    userServiceCount <- userServiceCount + 1
                    printfn "%s going offline" userID'
                    offlineUserIDsSet <- Set.add userID' offlineUserIDsSet
                    followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
                    //cprinters.[cid] <! sprintf "[%s][OFFLINE] User %s is going offline" (timestamp.ToString()) uid
            return! loop()
        }
        loop()

    let TweetActor (mailbox:Actor<_>) = 
        let mutable tweetCount = 0
        let mutable userTweetCountMap = Map.empty
        let mutable usersActor = mailbox.Self

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | InitTweetsActor (usersActor') ->
                usersActor <- usersActor'
            | Tweet (clientID', userID', tweet', time')->
                tweetCount <- tweetCount + 1
                printfn "Tweet: %s" tweet'
                let mutable twCount = 0
                //cprinters.[cid] <! sprintf "[%s][TWEET] %s" (timestamp.ToString()) twt //TODO

                if userTweetCountMap.ContainsKey userID' then 
                    twCount <- userTweetCountMap.[userID'] + 1
                userTweetCountMap <- Map.add userID' twCount userTweetCountMap

                // Update Feeds //TODO
                //usersActor <! {messageName="UpdateFeeds"; clientID=msg.clientID; userID=msg.userID; tweet=msg.tweet; actionType = "tweet"; time=DateTime.Now;}
                
                // These are performance metrics //TODO
                //twTotalTime <- twTotalTime + (timestamp.Subtract reqTime).TotalMilliseconds
                //let averageTime = twTotalTime / tweetCount
                //boss <! ("ServiceStats","","Tweet",(averageTime |> string),DateTime.Now) 
            | UpdateTweetClientPrinters (updateMap') ->
                printfn "Updating Tweet Map"
                //cprinters <- updateMap'
            | IncreaseTweetCount (userID') ->
                printfn "Increasing Tweet Count"
                if userTweetCountMap.ContainsKey userID' then
                    let newTweetCount = userTweetCountMap.[userID'] + 1
                    userTweetCountMap <- Map.remove userID' userTweetCountMap
                    userTweetCountMap <- Map.add userID' newTweetCount userTweetCountMap
            | PrintTweetInfo (followers', requestedStats', performance') ->
                printfn "Printing Tweet Info"
                (*
                    File.WriteAllText(path, "")
                    File.AppendAllText(path, ("\n"+timestamp.ToString()))
                    File.AppendAllText(path, (sprintf "\nNumber of user requests handled per second = %u\n" perf))
                    File.AppendAllText(path, "\nAverage time taken for service(s) in ms:")
                    for stat in reqStats do
                        File.AppendAllText(path, (sprintf "\n%s = %s" stat.Key stat.Value))
                    let headers = "\n\nUserID\t#Followers\t#Tweets\n"
                    File.AppendAllText(path, headers)
                    for uid in followings do
                        if usersTweetCount.ContainsKey uid.Key then
                            let stat = sprintf "%s\t%s\t%s\n" uid.Key (uid.Value.Count |> string) (usersTweetCount.[uid.Key] |> string)
                            File.AppendAllText(path, stat) 
                *)
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let MentionsActor (mailbox:Actor<_>) = 
        let mutable usersSet = Set.empty
        let mutable userMentionsListMap = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | RegisterUserWithMentionsActor (clientID', userID') ->
                usersSet <- Set.add userID' usersSet
                userMentionsListMap <- Map.add userID' List.empty userMentionsListMap
            | Tweet(clientID', userID', tweet', time') ->
                let tweetSplit = tweet'.Split ' '
                for word in tweetSplit do
                    if word.[0] = '@' then
                        let mention = word.[1..(word.Length-1)]
                        if usersSet.Contains mention then
                            let mutable mentionsList = userMentionsListMap.[word.[1..(word.Length-1)]]
                            mentionsList <- tweet' :: mentionsList
                            userMentionsListMap <- Map.add mention mentionsList userMentionsListMap
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let HashtagsActor (mailbox:Actor<_>) = 
        let mutable hashtagTweetsListMap = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | Tweet(clientID', userID', tweet', time') ->
                let tweetSplit = tweet'.Split ' '
                for word in tweetSplit do
                    if word.[0] = '#' then
                        let hashtag = word.[1..(word.Length-1)]
                        if not (hashtagTweetsListMap.ContainsKey hashtag) then
                            hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap

                        let mutable tweetsList = hashtagTweetsListMap.[hashtag]
                        tweetsList <- tweet' :: tweetsList
                        hashtagTweetsListMap <- Map.add hashtag tweetsList hashtagTweetsListMap
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
            let! message = mailbox.Receive() 
            match message with
            | InitRetweetsActor(userActor', tweetsActor') ->
                usersActor <- userActor'
                tweetActor <- tweetsActor'
            | UpdateRetweetClientPrinters(updateMap') ->
                printfn "Updating Retweet Client Printers"
            |  UpdateRetweetFeedTable(id, _, tweet) -> //TODO
                printfn "Updating Retweet Feed Table"
                (*let mutable feedTableList = []
                if feedtable.ContainsKey id then
                    feedTableList <- feedtable.[id]
                feedTableList <- tweet :: feedTableList
                feedtable <- Map.remove id feedtable
                feedtable <- Map.add id feedTableList feedtable*)
            | Retweet(clientID, userID, time) ->
                printfn "retweeting" // TODO
                (*if feedtable.ContainsKey userID then
                    printfn "Finally made it inside here" // Working to get this printing
                    retweetCount <- retweetCount + 1.0
                    let randTweet = feedtable.[userID].[Random().Next(feedtable.[userID].Length)]
                    //cprinters.[clientID] <! sprintf "[%s][RE_TWEET] %s retweeted -> %s" (timeStamp.ToString()) userID randTweet
                    //reTweetTime <- reTweetTime + (timeStamp.Subtract time).TotalMilliseconds
                    //let averageTime = reTweetTime / reTweetCount
                    //mailbox.Sender() <! ("ServiceStats","","ReTweet",(averageTime |> string),DateTime.Now)
                    usersActor <! {messageName="UpdateFeeds"; clientID=clientID; userID=userID; tweet=randTweet; actionType="retweeted"; time=DateTime.Now}
                    //tweetActor <! IncTweet(uid) // Increment a user's tweet count on the tweet actor*)
            return! loop()
        }
        loop()

    let ShowFeedActor (mailbox:Actor<_>) = 
        //let mutable clientPrinter = Map.empty
        let mutable feedtable = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive()
            let timestamp = DateTime.Now
            match message with
            (*| UpdateShowFeedClientPrinters(ob) ->
                cprinters <- ob
            | ShowFeeds(cid, uid, cadmin) ->
                if feedtable.ContainsKey uid then
                    let mutable feedsTop = ""
                    let mutable fSize = 10
                    let feedList:List<string> = feedtable.[uid]
                    if feedList.Length < 10 then
                        fSize <- feedList.Length
                    for i in [0..(fSize-1)] do
                        feedsTop <- "\n" + feedtable.[uid].[i]
                    cprinters.[cid] <! sprintf "[%s][ONLINE] User %s is online..Feeds -> %s" (timestamp.ToString()) uid feedsTop
                else
                    cprinters.[cid] <! sprintf "[%s][ONLINE] User %s is online..No feeds yet!!!" (timestamp.ToString()) uid
                cadmin <! ("AckOnline", uid, "", "", "")*)
            | UpdateFeedTable (clientID', userID', tweet')->
                printfn "updating feed table"
                (*let mutable tmpList = []
                if feedtable.ContainsKey msg.followerID then //TODO
                    tmpList <- feedtable.[msg.followerID]
                tmpList  <- msg.tweet :: tmpList
                feedtable <- Map.remove msg.followerID feedtable
                feedtable <- Map.add msg.followerID tmpList feedtable*)
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
        let mutable showfeedActor = null

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let timestamp = DateTime.Now
            let (messageType, p1, p2, p3, time) : Tuple<string,string,string,string,DateTime> = downcast message
            match messageType with
            | "StartServer" ->
                usersActor <- spawn system ("UsersActor") UsersActor
                tweetsActor <- spawn system ("TweetActor") TweetActor
                mentionsActor <- spawn system ("MentionsActor") MentionsActor
                hashtagsActor <- spawn system ("HashtagsActor") HashtagsActor
                retweetsActor <- spawn system ("RetweetsActor") RetweetsActor
                showfeedActor <- spawn system ("ShowFeedActor") ShowFeedActor

                tweetsActor <! InitTweetsActor(usersActor)
                usersActor <! InitUsersActor(showfeedActor, retweetsActor)
                retweetsActor <! InitRetweetsActor(usersActor, tweetsActor)

                printfn "Server Started"
            | "ClientRegistration" ->
                requestsCount <- requestsCount + 1UL
                //let clientPrinter = system.ActorSelection(sprintf "akka.tcp://TwitterClient@%s:%s/user/Printer" msg.IP msg.port)
                //clientPrinters <- Map.add msg.ID clientPrinter clientPrinters //TODO
                //sendToAllActors clientprinters
                let message = "[" + timestamp.ToString() + "][CLIENT_REGISTER] Client " + p1 + " registered with server"
                mailbox.Sender() <! ("ClientRegister", message, "", "", "")
            | "UserRegistration" -> // clientID, userID, followers, DateTime.Now)
                usersActor <! RegisterUserWithUsersActor(p2, p3, time)
                mentionsActor <! RegisterUserWithMentionsActor(p1, p2)
                requestsCount <- requestsCount + 1UL
                let message = "[" + timestamp.ToString() + "][USER_REGISTER] User " + p2 + " registered with server"
                mailbox.Sender() <! ("UserRegister", p2, message, "", "")
            | "Tweet" ->
                requestsCount <- requestsCount + 1UL
                mentionsActor <! Tweet(p1, p2, p3, time) // forward tweet to mentions Actor
                hashtagsActor <! Tweet(p1, p2, p3, time) // forward tweet to hashtags Actor
                tweetsActor <! Tweet(p1, p2, p3, time) // forward tweet to tweets Actor
            | "Retweet"  ->
                requestsCount <- requestsCount + 1UL
                retweetsActor <! Retweet(p1, p2, time) // forward tweet to retweets Actor
            | "ServerFollow" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! ClientFollow(p1, p2, p3, time) // forward tweet to users Actor
            | "ServerGoOffline" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOffline(p1, p2, time)
            | "ServerGoOnline" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOnline(p1, p2, mailbox.Sender(), time)
            | "QueryTheMentions" ->
                printfn "Querying mentions"
            | "QueryTheHashtags" ->
                printfn "Querying hashtags"
            | "DisplayTotalStats" ->
                printfn "Displaying server stats"
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let boss = spawn system "ServerActor" ServerActor

    boss <! ("StartServer", "", "", "", DateTime.Now)

    system.WhenTerminated.Wait()

if "client" = (fsi.CommandLineArgs.[1] |> string) then
    // Command Line input
    let myIP = fsi.CommandLineArgs.[2] |> string
    let port = fsi.CommandLineArgs.[3] |> string
    let ID = fsi.CommandLineArgs.[4] |> string
    let usersCount = fsi.CommandLineArgs.[5] |> string
    let clientsCount = fsi.CommandLineArgs.[6] |> string
    let serverIP = fsi.CommandLineArgs.[7] |> string

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
        let mutable userID = ""
        let mutable clientID = ""
        let mutable server = ActorSelection()
        let mutable usersCount = 0
        let mutable tweetCount = 0
        let mutable interval = 0.0
        let mutable clientsList = []
        let mutable hashtagsList = []

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
                    let tweetTypes =  [1..5]
                    let tweetType = tweetTypes.[Random().Next(tweetTypes.Length)]
                    let mutable tweet = ""
                    match tweetType with   
                    | 1 ->  // tweet without mention or hashtag
                        tweetCount <- tweetCount + 1
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount)
                        server <! ("Tweet", clientID, userID, tweet, DateTime.Now)
                    | 2 -> // tweet with mention
                        tweetCount <- tweetCount + 1
                        let mutable mUser = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclid = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randclid +  "_" + mUser
                        while mentionsUser = userID do 
                            mUser <- [1..usersCount].[Random().Next(usersCount)] |> string
                            mentionsUser <- randclid +  "_" + mUser
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with mention @" + mentionsUser
                        server <! ("Tweet", clientID, userID, tweet, DateTime.Now)
                    | 3 -> // tweet with hashtag
                        tweetCount <- tweetCount + 1
                        let hashtag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        tweet <- userID + " tweeted -> tweet_" + string(tweetCount) + " with hashtag #" + hashtag
                        server <! ("Tweet", clientID, userID, tweet, DateTime.Now)
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
                        server <! ("Tweet", clientID, userID, tweet, DateTime.Now)
                    | 5 -> // retweet
                        server <! ("Retweet", clientID, userID, tweet, DateTime.Now)
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(interval), mailbox.Self, StartTweet)  
            | StartOtherAction ->
                if online then
                    let actionTypes =  [1..5]
                    let actionType = actionTypes.[Random().Next(actionTypes.Length)]
                    match actionType with
                    | 1 ->  // follow
                        let mutable userToFollow = [1 .. usersCount].[Random().Next(usersCount)] |> string
                        let mutable randClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable userToFollowID = randClientID + "_" + userToFollow
                        while userToFollowID = userID do 
                            userToFollow <- [1 .. usersCount].[Random().Next(usersCount)] |> string
                            userToFollowID <- randClientID + "_" + userToFollow
                        server <! ("ServerFollow", clientID, userID, userToFollowID, DateTime.Now)
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
                        server <! ("QueryTheHashtags", clientID, userID, "", DateTime.Now)
                    | 5 ->  // query mentions
                        printfn "querying mentions"
                        server <! ("QueryTheMentions", clientID, userID, "", DateTime.Now)
                        (*let mutable mUser = [1 .. usersCount].[mentionsRandReq.Next(usersCount)] |> string
                        let mutable randclid = clientList.[clientRand.Next(clientList.Length)]
                        let mutable mentionsUser = sprintf "%s_%s" randclid mUser
                        server <! ("QueryMentions",cliId,myId,mentionsUser,DateTime.Now)*)
                        //TODO
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, StartOtherAction)
            | UserActorGoOffline ->
                online <- false
            return! loop()
        }
        loop()

    let ClientActor (mailbox:Actor<_>) = 
        let mutable clientID = ""
        let mutable usersCount = 0
        let mutable clientsList = []
        let mutable usersList = []
        let mutable registeredUserIDsList = []
        let mutable userIntervalMap = Map.empty
        let mutable userFollowersRankMap = Map.empty
        let mutable userIDUserActorRefMap = Map.empty
        let mutable currentlyOfflineUsersSet = Set.empty
        let server = system.ActorSelection("akka.tcp://Server@" + serverIP + ":8776/user/ServerActor")
        let hashtagsList = ["VenmoItForward"; "SEIZED"; "WWE2K22"; "JusticeForJulius"; "AhmaudArbery"; "NASB"; "Ticketmaster"; "gowon"; "Stacy"; "Garnet"; "Gaetz"; "Accused"; "Omarova"; "Cenk"; "McMuffin";]

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let (messageType, p1, p2, p3, p4) : Tuple<string,string,string,string,string> = downcast message
            match messageType with
            | "StartClient" ->
                // Get number of users, client ID, and set users array
                usersCount <- int32(p2)
                clientID <- p1
                let mutable usersArray= [| 1 .. int32(p2) |]

                // Print info
                printerRef <! "Client " + clientID + " Start!"
                printerRef <! "Number of users: " + string(usersCount)
                
                // Shuffle around users in users array
                let swap (a: _[]) x y =
                    let tmp = a.[x]
                    a.[x] <- a.[y]
                    a.[y] <- tmp
                let rand = new System.Random()
                let shuffle a = Array.iteri (fun i _ -> swap a i (rand.Next(i, Array.length a))) a
                shuffle usersArray
                usersList <- usersArray |> Array.toList

                // Making second user array
                for i in [1 .. (p2 |> int32)] do
                    let userKey = usersArray.[i-1] |> string
                    userFollowersRankMap <- Map.add (clientID + "_" + userKey) ((int32(p2)-1)/i) userFollowersRankMap
                    userIntervalMap <- Map.add (clientID  + "_" + userKey) i userIntervalMap
                server <! ("ClientRegistration", clientID, "", "", DateTime.Now)
                for i in [1 .. int32(p3)] do
                    clientsList <- (i |> string) :: clientsList
            | "ClientRegister" ->
                // Send message to register user and time an offline simulation
                printerRef <! p1
                mailbox.Self <! ("RegisterUser", "1", "", "", "")
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))
            | "RegisterUser" ->
                let mutable userID = clientID  + "_" + (usersList.[int32(p1)-1] |> string)
                let userRef = spawn system ("User_" + userID) UserActor
                userIDUserActorRefMap <- Map.add userID userRef userIDUserActorRefMap
                let followers = userFollowersRankMap.[userID] |> string
                server <! ("UserRegistration", clientID, userID, followers, DateTime.Now)
                registeredUserIDsList <- userID :: registeredUserIDsList
                if int32(p1) < usersCount then
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(50.0), mailbox.Self, ("RegisterUser", string(int32(p1) + 1), "", "", ""))
            | "UserRegister" ->
                printerRef <! p2
                let mutable baseInterval = usersCount/100
                if baseInterval < 5 then
                    baseInterval <- 5
                userIDUserActorRefMap.[p1] <! Ready(p1, clientsList, server, usersCount, clientID, hashtagsList, (baseInterval*userIntervalMap.[p1]))
            | "ClientActorGoOffline" ->
                let mutable totalUsers = registeredUserIDsList.Length
                totalUsers <- (30*totalUsers)/100
                let mutable tempOfflineUserIDsSet = Set.empty

                for i in [1 .. totalUsers] do
                    let mutable nextOfflineUserID = registeredUserIDsList.[Random().Next(registeredUserIDsList.Length)]
                    
                    while currentlyOfflineUsersSet.Contains(nextOfflineUserID) || tempOfflineUserIDsSet.Contains(nextOfflineUserID) do
                        nextOfflineUserID <- registeredUserIDsList.[Random().Next(registeredUserIDsList.Length)]
                    
                    server <! ("ServerGoOffline", clientID, nextOfflineUserID, "", DateTime.Now)
                    userIDUserActorRefMap.[nextOfflineUserID] <! UserActorGoOffline
                    tempOfflineUserIDsSet <- Set.add nextOfflineUserID tempOfflineUserIDsSet

                for ID in currentlyOfflineUsersSet do
                    server <! ("ServerGoOnline", clientID, ID, "", DateTime.Now)
                    
                currentlyOfflineUsersSet <- tempOfflineUserIDsSet
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))

                printfn "client going offline"
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    // Start - spawn boss
    let boss = spawn system "AdminActor" ClientActor

    boss <! ("StartClient", ID, usersCount, clientsCount, port)

    system.WhenTerminated.Wait()
