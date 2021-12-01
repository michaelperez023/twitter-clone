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
    | UserActorGoOnline

type UsersActorMessages = 
    | InitUsersActor of IActorRef * IActorRef
    | RegisterUserWithUsersActor of string * string
    | Follow of string * string * string
    | Unfollow of string * string
    | UsersActorGoOnline of string * string * IActorRef
    | UsersActorGoOffline of string * string
    | UpdateFeeds of string * string * string * string

type RetweetsActorMessages = 
    | InitRetweetsActor of IActorRef * IActorRef
    | UpdateRetweetFeedTable of string * string
    | Retweet of string * string

type ShowFeedActorMessages = 
    | UpdateFeedTable of string * string
    | ShowFeeds of string * string * IActorRef * IActorRef

type TweetsHashtagsMentionsActorsMessages = 
    | IDAndTweet of string * string * string
    | Tweet of string
    | RegisterUserWithMentionsActor of string * string
    | IncrementTweetCount of string
    | QueryHashtag of string * string * string
    | QueryMention of string * string * string

if "server" = (fsi.CommandLineArgs.[1] |> string) then
    let system = ActorSystem.Create("Server", ConfigurationFactory.ParseString(@"akka { actor { provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote"" }
                                remote.helios.tcp { 
                                    transport-protocol = tcp
                                    port = 8776
                                    hostname = " + (fsi.CommandLineArgs.[2] |> string) + "
                                } }"))

    let UsersActor (mailbox:Actor<_>) = 
        let mutable userFollowersCountMap = Map.empty
        let mutable userUsersFollowingSetMap = Map.empty
        let mutable showfeedActor = null
        let mutable retweetActor = null
        let mutable offlineUserIDsSet = Set.empty
        let mutable server = null

        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | InitUsersActor (actor1, actor2) ->
                showfeedActor <- actor1
                retweetActor <- actor2
                server <- mailbox.Sender()
            | RegisterUserWithUsersActor (userID', followersCount') ->
                // Get the amount of followers a user has and how many that user is following
                userFollowersCountMap <- Map.add userID' (followersCount' |> int) userFollowersCountMap
                userUsersFollowingSetMap <- Map.add userID' Set.empty userUsersFollowingSetMap
            | Follow (clientID', userID', userToFollowID') ->
                // Follow another account as long as it is not the user themselves
                if userUsersFollowingSetMap.ContainsKey userToFollowID' && not (userUsersFollowingSetMap.[userToFollowID'].Contains userToFollowID') && userUsersFollowingSetMap.[userToFollowID'].Count < userFollowersCountMap.[userToFollowID'] then
                    let mutable tempUsersFollowingSet = userUsersFollowingSetMap.[userID']
                    tempUsersFollowingSet <- Set.add userToFollowID' tempUsersFollowingSet
                    userUsersFollowingSetMap <- Map.add userID' tempUsersFollowingSet userUsersFollowingSetMap

                    let message = "[" + DateTime.Now.ToString() + "] [FOLLOW] user " + clientID' + " followed " + userToFollowID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            | Unfollow (clientID', userID') ->
                let tempUsersFollowingList = Set.fold (fun l se -> se::l) [] (userUsersFollowingSetMap.Item(userID'))
                if tempUsersFollowingList.Length > 0 then
                    let userToUnfollowID' = tempUsersFollowingList.[Random().Next(tempUsersFollowingList.Length)]

                    let message = "[" + DateTime.Now.ToString() + "] [UNFOLLOW] user " + clientID' + " unfollowed " + userToUnfollowID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            | UpdateFeeds (clientID', userID', tweet', type') ->
                for clientID in userUsersFollowingSetMap.[userID'] do
                    showfeedActor <! UpdateFeedTable(userID', tweet')
                    retweetActor <! UpdateRetweetFeedTable(userID', tweet')
                    if not (offlineUserIDsSet.Contains clientID') then
                        let userIDSplit = clientID.Split '_'
                        let clientIDToSendTo = userIDSplit.[0]
                        if type' = "tweet" then
                            let message = "[" + DateTime.Now.ToString() + "] [FEED_TWEET] for user " + clientID + ": '" + tweet' + "'"
                            server <! ("ClientPrint", clientIDToSendTo, userID', message)
                        else // retweet
                            let message = "[" + DateTime.Now.ToString() + "] [FEED_RETWEET] for user " + clientID + ": '" + tweet' + "'"
                            server <! ("ClientPrint", clientIDToSendTo, userID', message)
            | UsersActorGoOnline (clientID', userID', cAdmin') ->
                    offlineUserIDsSet <- Set.remove userID' offlineUserIDsSet
                    showfeedActor <! ShowFeeds(clientID', userID', cAdmin', mailbox.Sender())
            | UsersActorGoOffline (clientID', userID') ->
                    offlineUserIDsSet <- Set.add userID' offlineUserIDsSet

                    let message = "[" + DateTime.Now.ToString() + "][OFFLINE] User " + userID' + " is going offline"
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            return! loop()
        }
        loop()

    let TweetActor (mailbox:Actor<_>) = 
        let mutable tweetCount = 0
        let mutable userTweetCountMap = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | IDAndTweet (clientID', userID', tweet')->
                tweetCount <- tweetCount + 1
                let mutable tweetCounter = 0

                if userTweetCountMap.ContainsKey userID' then 
                    tweetCounter <- userTweetCountMap.[userID'] + 1
                userTweetCountMap <- Map.add userID' tweetCounter userTweetCountMap

                let message = "[" + DateTime.Now.ToString() + "] [TWEET] " + tweet'
                mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            | IncrementTweetCount (userID') ->
                if userTweetCountMap.ContainsKey userID' then 
                    let tweetCounter = (userTweetCountMap.[userID'] + 1)
                    userTweetCountMap <- Map.add userID' tweetCounter userTweetCountMap
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
            | Tweet (tweet') ->
                for word in tweet'.Split ' ' do
                    if word.[0] = '@' then
                        if usersSet.Contains word.[1..(word.Length-1)] then
                            let mutable mentionsList = userMentionsListMap.[word.[1..(word.Length-1)]]
                            mentionsList <- tweet' :: mentionsList
                            userMentionsListMap <- Map.add word.[1..(word.Length-1)] mentionsList userMentionsListMap
            | QueryMention (clientID', userID', mentionedUserID') ->
                if userMentionsListMap.ContainsKey mentionedUserID' then
                    let mutable mentionSize = userMentionsListMap.[mentionedUserID'].Length
                    if (mentionSize > 10) then
                        mentionSize <- 10
                    let mutable tweets = ""
                    for i in [0..(mentionSize-1)] do
                        tweets <- "\n" + userMentionsListMap.[mentionedUserID'].[i]
                    let message = "[" + DateTime.Now.ToString() + "] [QUERY_MENTION] by user " + userID' + " - recent tweets for user @" + mentionedUserID' + ": " + tweets
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                else
                    let message = "[" + DateTime.Now.ToString() + "] [QUERY_MENTION] by user " + userID' + " - no tweets for user @" + mentionedUserID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
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
            | Tweet (tweet') ->
                for word in tweet'.Split ' ' do
                    if word.[0] = '#' then
                        let hashtag = word.[1..(word.Length-1)]
                        if not (hashtagTweetsListMap.ContainsKey hashtag) then
                             hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap

                        let mutable tweetsList = hashtagTweetsListMap.[word.[1..(word.Length-1)]]
                        tweetsList <- tweet' :: tweetsList
                        hashtagTweetsListMap <- Map.add hashtag tweetsList hashtagTweetsListMap
            | QueryHashtag (clientID', userID', hashtag') ->
                if hashtagTweetsListMap.ContainsKey hashtag' then
                    let mutable hashtagSize = hashtagTweetsListMap.[hashtag'].Length
                    if (hashtagSize > 10) then
                        hashtagSize <- 10
                    let mutable hashtags = ""
                    for i in [0..(hashtagSize-1)] do
                        hashtags <- hashtags + "\n" + hashtagTweetsListMap.[hashtag'].[i]

                    let message = "[" + DateTime.Now.ToString() + "] [QUERY_HASHTAG] by user " + userID' + " - recent 10 (maximum) tweets that have hashtag #" + hashtag' + ": " + hashtags
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                else
                    let message = "[" + DateTime.Now.ToString() + "] [QUERY_HASHTAG] by user " + userID' + " - no tweets have hashtag #" + hashtag'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let RetweetsActor (mailbox:Actor<_>) = 
        let mutable userIDFeedListMap = Map.empty
        let mutable retweetCount = 0
        let mutable usersActor = mailbox.Self
        let mutable tweetActor = mailbox.Self

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | InitRetweetsActor(userActor', tweetsActor') ->
                usersActor <- userActor'
                tweetActor <- tweetsActor'
            |  UpdateRetweetFeedTable(userID', tweet') ->
                let mutable feedTableList = []
                if userIDFeedListMap.ContainsKey userID' then
                    feedTableList <- userIDFeedListMap.[userID']
                feedTableList <- tweet' :: feedTableList
                userIDFeedListMap <- Map.add userID' feedTableList userIDFeedListMap
            | Retweet(clientID', userID') ->
                if userIDFeedListMap.ContainsKey userID' then
                    retweetCount <- retweetCount + 1
                    let randTweet = userIDFeedListMap.[userID'].[Random().Next(userIDFeedListMap.[userID'].Length)]
                    let message = "[" + DateTime.Now.ToString() + "] [RETWEET] by user " + userID' + ": " + randTweet
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                    
                    usersActor <! UpdateFeeds(clientID', userID', randTweet, "retweet")
                    tweetActor <! IncrementTweetCount(userID') // Increment user's tweet count
            return! loop()
        }
        loop()

    let ShowFeedActor (mailbox:Actor<_>) = 
        let mutable userIDFeedListMap = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | ShowFeeds(clientID', userID', cAdmin', server') ->
                if userIDFeedListMap.ContainsKey userID' then
                    let mutable feedsTop = ""
                    let mutable feedSize = 10
                    let feedList:List<string> = userIDFeedListMap.[userID']
                    if feedList.Length < 10 then
                        feedSize <- feedList.Length
                    for i in [0..(feedSize-1)] do
                        feedsTop <- "\n" + userIDFeedListMap.[userID'].[i]
                    
                    let message = "[" + DateTime.Now.ToString() + "][ONLINE] User " + userID' + " is online, feed: " + feedsTop
                    server' <! ("ClientPrint", clientID', userID', message)
                else
                    let message = "[" + DateTime.Now.ToString() + "][ONLINE] User " + userID' + " is online, no tweets in feed yet"
                    server' <! ("ClientPrint", clientID', userID', message)
                cAdmin' <! ("AckOnline", userID', "", "", "")
            | UpdateFeedTable (userID', tweet')->
                let mutable tempList = []
                if userIDFeedListMap.ContainsKey userID' then
                    tempList <- userIDFeedListMap.[userID']
                tempList  <- tweet' :: tempList
                userIDFeedListMap <- Map.add userID' tempList userIDFeedListMap
            return! loop()
        }
        loop()

    let ServerActor (mailbox:Actor<_>) = 
        let mutable requestsCount = 0UL
        let mutable usersActor = null
        let mutable tweetsActor = null
        let mutable mentionsActor = null
        let mutable hashtagsActor = null
        let mutable retweetsActor = null
        let mutable showfeedActor = null
        let mutable clientIDClientPrinterMap = Map.empty
        let start = DateTime.Now

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let (messageType, p1, p2, p3) : Tuple<string,string,string,string> = downcast message
            match messageType with
            | "StartServer" ->
                printfn "Server Started"
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(3.), mailbox.Self, ("StatsPrinter","","",""))
                usersActor <- spawn system ("UsersActor") UsersActor
                tweetsActor <- spawn system ("TweetActor") TweetActor
                retweetsActor <- spawn system ("RetweetsActor") RetweetsActor
                hashtagsActor <- spawn system ("HashtagsActor") HashtagsActor
                mentionsActor <- spawn system ("MentionsActor") MentionsActor
                showfeedActor <- spawn system ("ShowFeedActor") ShowFeedActor

                // Send actors the references
                usersActor <! InitUsersActor(showfeedActor, retweetsActor)
                retweetsActor <! InitRetweetsActor(usersActor, tweetsActor)
            | "ClientRegistration" ->
                requestsCount <- requestsCount + 1UL
                let clientPrinterActorSelection = system.ActorSelection("akka.tcp://Client@" + p2 + ":" + p3 + "/user/Printer")
                clientIDClientPrinterMap <- Map.add p1 clientPrinterActorSelection clientIDClientPrinterMap

                let message = "[" + DateTime.Now.ToString() + "][CLIENT_REGISTER] Client " + p1 + " registered with server"
                mailbox.Sender() <! ("AckClientReg", message, "", "", "")
            | "UserRegistration" -> // clientID, userID, followers, DateTime.Now
                usersActor <! RegisterUserWithUsersActor(p2, p3)
                mentionsActor <! RegisterUserWithMentionsActor(p1, p2)
                requestsCount <- requestsCount + 1UL

                let message = "[" + DateTime.Now.ToString() + "] [USER_REGISTER] User " + p2 + " registered with server"
                mailbox.Sender() <! ("AckUserReg", p2, message, "", "")
            | "Tweet" ->
                requestsCount <- requestsCount + 1UL
                mentionsActor <! Tweet(p3) // forward tweet to mentions Actor
                hashtagsActor <! Tweet(p3) // forward tweet to hashtags Actor
                tweetsActor <! IDAndTweet(p1, p2, p3) // forward tweet to tweets Actor
                usersActor <! UpdateFeeds(p1, p2, p3, "tweet")
            | "Retweet"  ->
                requestsCount <- requestsCount + 1UL
                retweetsActor <! Retweet(p1, p2) // forward tweet to retweets Actor
            | "Follow" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! Follow(p1, p2, p3) // forward to users Actor
            | "Unfollow" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! Unfollow(p1, p2) // forward tweet to users Actor
            | "ServerGoOffline" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOffline(p1, p2)
            | "ServerGoOnline" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOnline(p1, p2, mailbox.Sender())
            | "QueryHashtag" ->
                requestsCount <- requestsCount + 1UL
                hashtagsActor <! QueryHashtag(p1, p2, p3)
            | "QueryMention" ->
                requestsCount <- requestsCount + 1UL
                mentionsActor <! QueryMention(p1, p2, p3)
            | "ClientPrint" ->
                requestsCount <- requestsCount + 1UL
                clientIDClientPrinterMap.[p1] <! p3
            | "StatsPrinter" ->
                printfn "Run Time = %us, Total Requests = %u" ((DateTime.Now.Subtract start).TotalSeconds |> uint64) requestsCount
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(3.), mailbox.Self, ("StatsPrinter", "", "", ""))
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let boss = spawn system "ServerActor" ServerActor // Start - spawn boss

    boss <! ("StartServer", "", "", "")

    system.WhenTerminated.Wait()

if "client" = (fsi.CommandLineArgs.[1] |> string) then
    let clientIP = fsi.CommandLineArgs.[2] |> string
    let clientPort = fsi.CommandLineArgs.[3] |> string
    let ID = fsi.CommandLineArgs.[4] |> string
    let usersCount = fsi.CommandLineArgs.[5] |> string
    let clientsCount = fsi.CommandLineArgs.[6] |> string
    let serverIP = fsi.CommandLineArgs.[7] |> string

    let system = ActorSystem.Create("Client", ConfigurationFactory.ParseString(@"akka { actor { provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""}
                                remote.helios.tcp {
                                    transport-protocol = tcp
                                    port = " + clientPort + "
                                    hostname = " + clientIP + "
                                } }"))

    let PrinterActor (mailbox:Actor<_>) = 
        let rec loop () = actor {
            let! message = mailbox.Receive()
            if message <> null then
                printfn "%s" message
            return! loop()
        }
        loop()

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
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.01), mailbox.Self, StartTweet)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.02), mailbox.Self, StartOtherAction)
            | StartTweet ->
                if online then
                    let tweetTypes =  [1..5]
                    let tweetType = tweetTypes.[Random().Next(tweetTypes.Length)]
                    let mutable tweet = ""
                    match tweetType with   
                    | 1 ->  // tweet without mention or hashtag
                        tweetCount <- tweetCount + 1
                        tweet <- "user " + userID + " tweeted tweet" + string(tweetCount)
                        server <! ("Tweet", clientID, userID, tweet)
                    | 2 -> // tweet with mention
                        tweetCount <- tweetCount + 1
                        let mutable mUser = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclid = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randclid +  "_" + mUser
                        while mentionsUser = userID do 
                            mUser <- [1..usersCount].[Random().Next(usersCount)] |> string
                            mentionsUser <- randclid +  "_" + mUser
                        tweet <- "user " + userID + " tweeted tweet" + string(tweetCount) + " with mention @" + mentionsUser
                        server <! ("Tweet", clientID, userID, tweet)
                    | 3 -> // tweet with hashtag
                        tweetCount <- tweetCount + 1
                        let hashtag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        tweet <- userID + " tweeted tweet" + string(tweetCount) + " with hashtag #" + hashtag
                        server <! ("Tweet", clientID, userID, tweet)
                    | 4 -> // tweet with mention and hashtag
                        tweetCount <- tweetCount + 1
                        let mutable mUser = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclid = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randclid +  "_" + mUser
                        while mentionsUser = userID do 
                            mUser <- [1..usersCount].[Random().Next(usersCount)] |> string
                            mentionsUser <- randclid +  "_" + mUser
                        let hashtag = hashtagsList.[Random().Next(hashtagsList.Length)]                    
                        tweet <- userID + " tweeted tweet" + string(tweetCount) + " with hashtag #" + hashtag + " and mentioned @" + mentionsUser
                        server <! ("Tweet", clientID, userID, tweet)
                    | 5 -> // retweet
                        server <! ("Retweet", clientID, userID, tweet)
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(interval), mailbox.Self, StartTweet)  
            | StartOtherAction ->
                if online then
                    let actionTypes =  [1..4]
                    let actionType = actionTypes.[Random().Next(actionTypes.Length)]
                    match actionType with
                    | 1 ->  // follow
                        let mutable userToFollow = [1 .. usersCount].[Random().Next(usersCount)] |> string
                        let mutable randClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable userToFollowID = randClientID + "_" + userToFollow
                        while userToFollowID = userID do 
                            userToFollow <- [1 .. usersCount].[Random().Next(usersCount)] |> string
                            userToFollowID <- randClientID + "_" + userToFollow
                        server <! ("Follow", clientID, userID, userToFollowID)
                    | 2 ->  // unfollow
                        server <! ("Unfollow", clientID, userID, "")
                    | 3 ->  // query hashtag
                        let hashTag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        server <! ("QueryHashtag", clientID, userID, hashTag)
                    | 4 ->  // query mention
                        let mutable mUser = [1 .. usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionedUserID = randclientID + "_" + mUser
                        server <! ("QueryMention", clientID, userID, mentionedUserID)
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.2), mailbox.Self, StartOtherAction)
            | UserActorGoOffline ->
                online <- false
            | UserActorGoOnline ->
                online <- true
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.1), mailbox.Self, Tweet)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.2), mailbox.Self, StartOtherAction)
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
        let printerRef = spawn system "Printer" PrinterActor
        let hashtagsList = ["VenmoItForward"; "SEIZED"; "WWE2K22"; "JusticeForJulius"; "AhmaudArbery"; 
        "NASB"; "Ticketmaster"; "gowon"; "Stacy"; "Garnet"; "Gaetz"; "Accused"; "Omarova"; "Cenk"; "McMuffin";
         "SpotifyWrapped"; "MinecraftManhunt"; "coolbills"; "SCOTUS"; "COP5615isgreat"; "OmicronVariant";
         "Watford"; "EVELIV"; "Wade"; "Kavanaugh"; "Drake"; "Hendo"; "WorldAidsDay2021"; "RoeVWade"; "Rihanna";
         "WhyIChime"; "Oxford"; "GunControlNow"; "Nets"; "Harden"; "Knicks"; "Holly"; "TheFlash"; "25DaysOfChristmas"; "Titanfall"]

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let (messageType, p1, p2, p3, p4) : Tuple<string,string,string,string,string> = downcast message
            match messageType with
            | "StartClient" ->
                usersCount <- int32(p2) // usersCount
                clientID <- p1 // clientID
                printerRef <! "Client " + clientID + " Start!"
                printerRef <! "Number of users: " + string(usersCount)

                let mutable usersArray= [| 1 .. int32(p2) |]
                let swap (a: _[]) x y =
                    let tmp = a.[x]
                    a.[x] <- a.[y]
                    a.[y] <- tmp
                let shuffle a = Array.iteri (fun i _ -> swap a i (Random().Next(i, Array.length a))) a

                shuffle usersArray
                usersList <- usersArray |> Array.toList
                for i in [1 .. (p2 |> int32)] do
                    let userKey = usersArray.[i-1] |> string
                    userFollowersRankMap <- Map.add (clientID + "_" + userKey) ((int32(p2)-1)/i) userFollowersRankMap
                    userIntervalMap <- Map.add (clientID  + "_" + userKey) i userIntervalMap
                server <! ("ClientRegistration", clientID, clientIP, p4)
                for i in [1 .. int32(p3)] do
                    clientsList <- (i |> string) :: clientsList
            | "AckClientReg" ->
                printerRef <! p1
                mailbox.Self <! ("RegisterUser", "1", "", "", "")
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10.), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))
            | "RegisterUser" ->
                let mutable userID = clientID  + "_" + (usersList.[int32(p1)-1] |> string)
                let userRef = spawn system ("User_" + userID) UserActor
                userIDUserActorRefMap <- Map.add userID userRef userIDUserActorRefMap
                let followers = userFollowersRankMap.[userID] |> string
                server <! ("UserRegistration", clientID, userID, followers)
                registeredUserIDsList <- userID :: registeredUserIDsList
                if int32(p1) < usersCount then
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.05), mailbox.Self, ("RegisterUser", string(int32(p1) + 1), "", "", ""))
            | "AckUserReg" ->
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
                    
                    server <! ("ServerGoOffline", clientID, nextOfflineUserID, "")
                    userIDUserActorRefMap.[nextOfflineUserID] <! UserActorGoOffline
                    tempOfflineUserIDsSet <- Set.add nextOfflineUserID tempOfflineUserIDsSet

                for ID in currentlyOfflineUsersSet do
                    server <! ("ServerGoOnline", clientID, ID, "")
                    
                currentlyOfflineUsersSet <- tempOfflineUserIDsSet
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10.), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))

                printfn "client going offline"
            | "AckOnline" ->
                userIDUserActorRefMap.[p1] <! UserActorGoOnline
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let boss = spawn system "AdminActor" ClientActor // Start - spawn boss

    boss <! ("StartClient", ID, usersCount, clientsCount, clientPort)

    system.WhenTerminated.Wait()
