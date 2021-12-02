#r "nuget: Akka"
#r "nuget: Akka.FSharp"
#r "nuget: Akka.Remote"

open System
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

type UserActorMessages = 
    | StartUserActor of (string*list<string>*ActorSelection*int*string*List<string>*int)
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
    | InitRetweetsActor of IActorRef
    | UpdateRetweetFeedTable of string * string
    | Retweet of string * string

type FeedActorMessages = 
    | UpdateFeedTable of string * string
    | ShowFeeds of string * string * IActorRef * IActorRef

type TweetsHashtagsMentionsActorsMessages = 
    | IDAndTweet of string * string * string
    | Tweet of string
    | RegisterUserWithMentionsActor of string
    | QueryHashtag of string * string * string
    | QueryMention of string * string * string

if "server" = (fsi.CommandLineArgs.[1] |> string) then
    let system = ActorSystem.Create("Server", ConfigurationFactory.ParseString(@"akka { actor { provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote"" }
                                remote.helios.tcp { 
                                    transport-protocol = tcp
                                    port = 4800
                                    hostname = " + (fsi.CommandLineArgs.[2] |> string) + "
                                } }"))

    let UsersActor (mailbox:Actor<_>) = 
        let mutable userFollowersCountMap = Map.empty
        let mutable userUsersFollowingSetMap = Map.empty
        let mutable feedActor = null
        let mutable retweetsActor = null
        let mutable offlineUserIDsSet = Set.empty
        let mutable server = null

        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | InitUsersActor (actor1, actor2) ->
                feedActor <- actor1
                retweetsActor <- actor2
                server <- mailbox.Sender()
            | RegisterUserWithUsersActor (userID', followersCount') ->
                // Get the amount of followers a user has and initialize empty usersFollowing set
                userFollowersCountMap <- Map.add userID' (followersCount' |> int) userFollowersCountMap
                userUsersFollowingSetMap <- Map.add userID' Set.empty userUsersFollowingSetMap
            | Follow (clientID', userID', userToFollowID') ->
                // Follow another account as long as it registered, not the same user, the user's followers is less than their followers count
                if userUsersFollowingSetMap.ContainsKey userToFollowID' && not (userUsersFollowingSetMap.[userToFollowID'].Contains userToFollowID') && userUsersFollowingSetMap.[userToFollowID'].Count < userFollowersCountMap.[userToFollowID'] then
                    let mutable tempUsersFollowingSet = userUsersFollowingSetMap.[userID']
                    tempUsersFollowingSet <- Set.add userToFollowID' tempUsersFollowingSet
                    userUsersFollowingSetMap <- Map.add userID' tempUsersFollowingSet userUsersFollowingSetMap

                    let message = "[" + DateTime.Now.ToString() + "] [FOLLOW] user " + clientID' + " followed " + userToFollowID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            | Unfollow (clientID', userID') ->
                let tempUsersFollowingList = Set.fold (fun l se -> se::l) [] (userUsersFollowingSetMap.Item(userID'))
                // Unfollow random user in the UsersFollowing set if the user is following any users
                if tempUsersFollowingList.Length > 0 then
                    let userToUnfollowID' = tempUsersFollowingList.[Random().Next(tempUsersFollowingList.Length)]

                    let message = "[" + DateTime.Now.ToString() + "] [UNFOLLOW] user " + clientID' + " unfollowed " + userToUnfollowID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            | UpdateFeeds (clientID', userID', tweet', type') ->
                // update the feeds for users that follow the user, for tweets and retweets, if the user is online
                for clientID in userUsersFollowingSetMap.[userID'] do
                    feedActor <! UpdateFeedTable(userID', tweet')
                    retweetsActor <! UpdateRetweetFeedTable(userID', tweet')
                    if not (offlineUserIDsSet.Contains clientID') then
                        let userIDSplit = clientID.Split '_'
                        let clientIDToSendTo = userIDSplit.[0]
                        if type' = "tweet" then
                            let message = "[" + DateTime.Now.ToString() + "] [FEED_TWEET] for user " + clientID + ": '" + tweet' + "'"
                            server <! ("ClientPrint", clientIDToSendTo, userID', message)

                        if type' = "retweet" then
                            let message = "[" + DateTime.Now.ToString() + "] [FEED_RETWEET] for user " + clientID + ": '" + tweet' + "'"
                            server <! ("ClientPrint", clientIDToSendTo, userID', message)
            | UsersActorGoOnline (clientID', userID', cAdmin') ->
                // Set the users Actor online, remove the corresponding userID from offlineUserIDsSet
                offlineUserIDsSet <- Set.remove userID' offlineUserIDsSet
                feedActor <! ShowFeeds(clientID', userID', cAdmin', mailbox.Sender())
            | UsersActorGoOffline (clientID', userID') ->
                // Set the users Actor offline, add the corresponding userID to offlineUserIDsSet
                offlineUserIDsSet <- Set.add userID' offlineUserIDsSet

                let message = "[" + DateTime.Now.ToString() + "] [OFFLINE] User " + userID' + " is going offline"
                mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
            return! loop()
        }
        loop()

    let TweetsActor (mailbox:Actor<_>) = 
        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | IDAndTweet (clientID', userID', tweet')->
                // Send tweet to client to be printed
                let message = "[" + DateTime.Now.ToString() + "] [TWEET] " + tweet'
                mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
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
            | RegisterUserWithMentionsActor (userID') ->
                usersSet <- Set.add userID' usersSet
                userMentionsListMap <- Map.add userID' List.empty userMentionsListMap
            | Tweet (tweet') ->
                // If the tweet contains an @ indicating a mention, add to userMentionsListMap
                for word in tweet'.Split ' ' do
                    if word.[0] = '@' then
                        if usersSet.Contains word.[1..(word.Length-1)] then
                            let mutable mentionsList = userMentionsListMap.[word.[1..(word.Length-1)]]
                            mentionsList <- tweet' :: mentionsList
                            userMentionsListMap <- Map.add word.[1..(word.Length-1)] mentionsList userMentionsListMap
            | QueryMention (clientID', userID', mentionedUserID') ->
                // If user has been mentioned in a tweet, print a maximum of 10 recent tweets that include the mention
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
                 // If tweet contains hashtag, save tweet in hashtagTweetsListMap
                for word in tweet'.Split ' ' do
                    if word.[0] = '#' then
                        let hashtag = word.[1..(word.Length-1)]
                        if not (hashtagTweetsListMap.ContainsKey hashtag) then
                             hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap

                        let mutable tweetsList = hashtagTweetsListMap.[word.[1..(word.Length-1)]]
                        tweetsList <- tweet' :: tweetsList
                        hashtagTweetsListMap <- Map.add hashtag tweetsList hashtagTweetsListMap
            | QueryHashtag (clientID', userID', hashtag') ->
                // If the hashtag has been in any tweets, print a 10 of maximum tweets that have the hashtag
                if hashtagTweetsListMap.ContainsKey hashtag' then
                    let mutable hashtagSize = hashtagTweetsListMap.[hashtag'].Length
                    if (hashtagSize > 10) then
                        hashtagSize <- 10

                    let mutable hashtags = ""
                    for i in [0..(hashtagSize-1)] do
                        hashtags <- hashtags + "\n" + hashtagTweetsListMap.[hashtag'].[i]

                    let message = "[" + DateTime.Now.ToString() + "] [QUERY_HASHTAG] by user " + userID' + " - recent 10 tweets that have hashtag #" + hashtag' + ": " + hashtags
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
        let mutable usersActor = null

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | InitRetweetsActor(userActor') ->
                usersActor <- userActor'
            | UpdateRetweetFeedTable(userID', tweet') ->
                // Update userIDFeedListMap for user
                let mutable feedList = []
                if userIDFeedListMap.ContainsKey userID' then
                    feedList <- userIDFeedListMap.[userID']
                feedList <- tweet' :: feedList
                userIDFeedListMap <- Map.add userID' feedList userIDFeedListMap
            | Retweet(clientID', userID') ->
                // If user if in userIDFeedListMap, print retweet and update the feeds for followers of the user
                if userIDFeedListMap.ContainsKey userID' then
                    let randTweet = userIDFeedListMap.[userID'].[Random().Next(userIDFeedListMap.[userID'].Length)]
                    let message = "[" + DateTime.Now.ToString() + "] [RETWEET] by user " + userID' + ": " + randTweet
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                    
                    usersActor <! UpdateFeeds(clientID', userID', randTweet, "retweet")
            return! loop()
        }
        loop()

    let FeedActor (mailbox:Actor<_>) = 
        let mutable userIDFeedListMap = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | ShowFeeds(clientID', userID', clientActor', server') ->
                // If the user has tweets in their feed, print a maximum of 10 recent tweets in their feed
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
                clientActor' <! ("ClientGoOnline", userID', "", "", "")
            | UpdateFeedTable (userID', tweet')->
                // Update the userIDFeedListMap with new tweet
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
        let mutable feedActor = null
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
                tweetsActor <- spawn system ("TweetsActor") TweetsActor
                retweetsActor <- spawn system ("RetweetsActor") RetweetsActor
                hashtagsActor <- spawn system ("HashtagsActor") HashtagsActor
                mentionsActor <- spawn system ("MentionsActor") MentionsActor
                feedActor <- spawn system ("FeedActor") FeedActor
                usersActor <! InitUsersActor(feedActor, retweetsActor)
                retweetsActor <! InitRetweetsActor(usersActor)
            | "ClientRegistration" -> // p1, p2, p3 = clientID, clientIP, port
                requestsCount <- requestsCount + 1UL
                let clientPrinterActorSelection = system.ActorSelection("akka.tcp://Client@" + p2 + ":" + p3 + "/user/Printer")
                clientIDClientPrinterMap <- Map.add p1 clientPrinterActorSelection clientIDClientPrinterMap

                let message = "[" + DateTime.Now.ToString() + "] [CLIENT_REGISTER] Client " + p1 + " registered with server"
                mailbox.Sender() <! ("ACKClientRegistration", message, "", "", "")
            | "UserRegistration" -> // p1, p2, p3 = clientID, userID, followersCount
                usersActor <! RegisterUserWithUsersActor(p2, p3)
                mentionsActor <! RegisterUserWithMentionsActor(p2)
                requestsCount <- requestsCount + 1UL

                let message = "[" + DateTime.Now.ToString() + "] [USER_REGISTER] User " + p2 + " registered with server"
                mailbox.Sender() <! ("ACKUserRegistration", p2, message, "", "")
            | "Tweet" -> // p1, p2, p3 = clientID, userID, tweet
                requestsCount <- requestsCount + 1UL
                mentionsActor <! Tweet(p3) // forward tweet to mentions Actor
                hashtagsActor <! Tweet(p3) // forward tweet to hashtags Actor
                tweetsActor <! IDAndTweet(p1, p2, p3) // forward tweet to tweets Actor
                usersActor <! UpdateFeeds(p1, p2, p3, "tweet")
            | "Retweet"  -> // p1, p2 = clientID, userID
                requestsCount <- requestsCount + 1UL
                retweetsActor <! Retweet(p1, p2) // forward tweet to retweets Actor
            | "Follow" -> // p1, p2 = clientID, userID, userToFollow
                requestsCount <- requestsCount + 1UL
                usersActor <! Follow(p1, p2, p3) // forward to users Actor
            | "Unfollow" -> // p1, p2 = clientID, userID
                requestsCount <- requestsCount + 1UL
                usersActor <! Unfollow(p1, p2) // forward tweet to users Actor
            | "ServerGoOffline" -> // p1, p2 = clientID, userID
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOffline(p1, p2) // start offline action
            | "ServerGoOnline" -> // p1, p2 = clientID, userID
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOnline(p1, p2, mailbox.Sender()) // start going back online action
            | "QueryHashtag" -> // p1, p2, p3= clientID, userID, hashtag
                requestsCount <- requestsCount + 1UL
                hashtagsActor <! QueryHashtag(p1, p2, p3) // find all tweets with hashtags
            | "QueryMention" -> // p1, p2, p3 = clientID, userID, mention
                requestsCount <- requestsCount + 1UL
                mentionsActor <! QueryMention(p1, p2, p3) // find all tweets with mentions
            | "ClientPrint" -> // p1, p2, p3 = clientID, userID, printMessage
                requestsCount <- requestsCount + 1UL
                clientIDClientPrinterMap.[p1] <! p3 // send messages to print to client
            | "StatsPrinter" ->
                printfn "Run Time = %us, Total Requests = %u" ((DateTime.Now.Subtract start).TotalSeconds |> uint64) requestsCount
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(3.), mailbox.Self, ("StatsPrinter", "", "", "")) // give server performance metrics
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
        let mutable tweetInterval = 0.0
        let mutable clientsList = []
        let mutable hashtagsList = []

        let rec loop () = actor {
            let! message = mailbox.Receive()
            match message with
            | StartUserActor(userID', clientsList', server', usersCount', clientID', hashtagsList', tweetInterval') ->
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(tweetInterval), mailbox.Self, StartTweet)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.02), mailbox.Self, StartOtherAction)
                userID <- userID'
                clientsList <- clientsList'
                server <- server'
                usersCount <- usersCount'
                clientID <- clientID'
                hashtagsList <- hashtagsList'
                tweetInterval <- tweetInterval' |> double
                online <- true
            | StartTweet ->
                if online then
                    let tweetTypes =  [1..5]
                    let mutable tweet = ""
                    // Randomly tweet different types of tweets
                    match tweetTypes.[Random().Next(tweetTypes.Length)] with   
                    | 1 ->  // Tweet without mention or hashtag
                        tweetCount <- tweetCount + 1
                        tweet <- "user " + userID + " tweeted tweet" + string(tweetCount)
                        server <! ("Tweet", clientID, userID, tweet)
                    | 2 -> // Tweet with mention and hashtag
                        tweetCount <- tweetCount + 1
                        let mutable randomClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randomClientID +  "_" + ([1..usersCount].[Random().Next(usersCount)] |> string)
                        while mentionsUser = userID do 
                            mentionsUser <- randomClientID +  "_" + ([1..usersCount].[Random().Next(usersCount)] |> string)

                        tweet <- userID + " tweeted tweet" + string(tweetCount) + " with hashtag #" + hashtagsList.[Random().Next(hashtagsList.Length)] + " and mentioned @" + mentionsUser
                        server <! ("Tweet", clientID, userID, tweet)
                    | 3 -> // Tweet with hashtag
                        tweetCount <- tweetCount + 1

                        tweet <- userID + " tweeted tweet" + string(tweetCount) + " with hashtag #" + hashtagsList.[Random().Next(hashtagsList.Length)]
                        server <! ("Tweet", clientID, userID, tweet)
                    | 4 -> // Tweet with mention
                        tweetCount <- tweetCount + 1
                        let mutable randomClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionsUser = randomClientID +  "_" + ([1..usersCount].[Random().Next(usersCount)] |> string)
                        while mentionsUser = userID do 
                            mentionsUser <- randomClientID +  "_" + ([1..usersCount].[Random().Next(usersCount)] |> string)

                        tweet <- "user " + userID + " tweeted tweet" + string(tweetCount) + " with mention @" + mentionsUser
                        server <! ("Tweet", clientID, userID, tweet)
                    | 5 -> // Retweet
                        server <! ("Retweet", clientID, userID, tweet)
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(tweetInterval), mailbox.Self, StartTweet)  
            | StartOtherAction ->
                if online then
                    let actionTypes =  [1..4]
                    // Randomly perform different actions
                    match actionTypes.[Random().Next(actionTypes.Length)] with
                    | 1 ->  // Query random hashtag
                        let hashTag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        server <! ("QueryHashtag", clientID, userID, hashTag)
                    | 2 ->  // Unfollow random user
                        server <! ("Unfollow", clientID, userID, "")
                    | 3 ->  // Follow random user
                        let mutable userToFollow = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randomClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable userToFollowID = randomClientID + "_" + userToFollow
                        while userToFollowID = userID do 
                            userToFollow <- [1..usersCount].[Random().Next(usersCount)] |> string
                            userToFollowID <- randomClientID + "_" + userToFollow
                        server <! ("Follow", clientID, userID, userToFollowID)
                    | 4 ->  // Query random mention
                        let mutable randomUserID = [1..usersCount].[Random().Next(usersCount)] |> string
                        let mutable randomClientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionedUserID = randomClientID + "_" + randomUserID
                        server <! ("QueryMention", clientID, userID, mentionedUserID)
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.2), mailbox.Self, StartOtherAction)
            | UserActorGoOnline ->
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.1), mailbox.Self, Tweet)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.2), mailbox.Self, StartOtherAction)
                online <- true
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
        let mutable userTweetFrequencyMap = Map.empty
        let mutable userFollowersCountMap = Map.empty
        let mutable userIDUserActorRefMap = Map.empty
        let mutable currentlyOfflineUsersSet = Set.empty
        let server = system.ActorSelection("akka.tcp://Server@" + serverIP + ":4800/user/ServerActor")
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
            | "StartClient" -> // p1, p2, p3, p4 = ID, usersCount, clientsCount, clientPort
                usersCount <- int32(p2)
                clientID <- p1

                let mutable usersArray= [|1..int32(p2)|]
                let swap (a: _[]) x y =
                    let tmp = a.[x]
                    a.[x] <- a.[y]
                    a.[y] <- tmp
                let shuffle a = Array.iteri (fun i _ -> swap a i (Random().Next(i, Array.length a))) a
                shuffle usersArray // Randomly shuffle usersArray
                usersList <- usersArray |> Array.toList
                
                // Initialize userFollowersCountMap and userTweetFrequencyMap, create zipf distribution of users' followers
                for i in [1..(p2 |> int32)] do
                    let userKey = usersArray.[i-1] |> string
                    userFollowersCountMap <- Map.add (clientID + "_" + userKey) ((int32(p2)-1)/i) userFollowersCountMap
                    userTweetFrequencyMap <- Map.add (clientID  + "_" + userKey) i userTweetFrequencyMap
                server <! ("ClientRegistration", clientID, clientIP, p4)
                for i in [1..int32(p3)] do 
                    clientsList <- (i |> string) :: clientsList // Initialize clientsList
                
                printerRef <! "Client " + clientID + " Start!"
                printerRef <! "Number of users: " + string(usersCount)
            | "ACKClientRegistration" ->
                // Acknowledge client registration, schedule the ClientActor going offline and register the first user
                printerRef <! p1
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10.), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))
                mailbox.Self <! ("RegisterUser", "0", "", "", "")
            | "RegisterUser" -> 
                // Register user and add to userIDUserActorRefMap and registeredUserIDsList, then register the next user
                let mutable userID = clientID  + "_" + (usersList.[int32(p1)] |> string)
                let userRef = spawn system ("User_" + userID) UserActor
                userIDUserActorRefMap <- Map.add userID userRef userIDUserActorRefMap
                let followersCount = userFollowersCountMap.[userID] |> string
                server <! ("UserRegistration", clientID, userID, followersCount)
                registeredUserIDsList <- userID :: registeredUserIDsList
                if int32(p1) < usersCount then
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(0.05), mailbox.Self, ("RegisterUser", string(int32(p1) + 1), "", "", ""))
            | "ACKUserRegistration" -> // p1, p2 = userID, message
                // Acknowledge user registration, initialize the minimum tweet interval 
                printerRef <! p2
                let mutable baseTweetFrequency = usersCount/100
                if baseTweetFrequency < 10 then
                    baseTweetFrequency <- 10
                userIDUserActorRefMap.[p1] <! StartUserActor(p1, clientsList, server, usersCount, clientID, hashtagsList, (baseTweetFrequency*userTweetFrequencyMap.[p1]))
            | "ClientActorGoOffline" ->
                // Set 10% of users to go offline, if they are not previously online and not a duplicate
                let mutable totalUsers = registeredUserIDsList.Length
                totalUsers <- (10 * totalUsers) / 100
                let mutable tempOfflineUserIDsSet = Set.empty

                for i in [1..totalUsers] do
                    let mutable nextOfflineUserID = registeredUserIDsList.[Random().Next(registeredUserIDsList.Length)]
                    
                    while currentlyOfflineUsersSet.Contains(nextOfflineUserID) || tempOfflineUserIDsSet.Contains(nextOfflineUserID) do
                        nextOfflineUserID <- registeredUserIDsList.[Random().Next(registeredUserIDsList.Length)]
                    
                    server <! ("ServerGoOffline", clientID, nextOfflineUserID, "")
                    userIDUserActorRefMap.[nextOfflineUserID] <! UserActorGoOffline
                    tempOfflineUserIDsSet <- Set.add nextOfflineUserID tempOfflineUserIDsSet

                // Set all previously offline users, online
                for ID in currentlyOfflineUsersSet do
                    server <! ("ServerGoOnline", clientID, ID, "")
                    
                currentlyOfflineUsersSet <- tempOfflineUserIDsSet
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(10.), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))
            | "ClientGoOnline" ->
                userIDUserActorRefMap.[p1] <! UserActorGoOnline
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let boss = spawn system "AdminActor" ClientActor // Start - spawn boss

    boss <! ("StartClient", ID, usersCount, clientsCount, clientPort)

    system.WhenTerminated.Wait()