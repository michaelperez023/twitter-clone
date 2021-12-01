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
    | RegisterUserWithUsersActor of string * string * DateTime
    | Follow of string * string * string * DateTime
    | Unfollow of string * string
    | UsersActorGoOnline of string * string * IActorRef * DateTime
    | UsersActorGoOffline of string * string * DateTime
    | UpdateFeeds of string * string * string * string * DateTime

type RetweetsActorMessages = 
    | InitRetweetsActor of IActorRef * IActorRef
    | UpdateRetweetFeedTable of string * string
    | Retweet of string * string * DateTime

type ShowFeedActorMessages = 
    | UpdateFeedTable of string * string
    | ShowFeeds of string * string * IActorRef * IActorRef

type TweetsHashtagsMentionsActorsMessages = 
    | IDAndTweet of string * string * string
    | Tweet of string
    | RegisterUserWithMentionsActor of string * string
    | IncrementTweetCount of string
    | QueryHashtag of string * string * string * DateTime
    | QueryMention of string * string * string * DateTime

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
        let mutable followTime = 0.0
        let mutable serviceCount = 0
        let mutable showfeedActor = null
        let mutable retweetActor = null
        let mutable offlineUserIDsSet = Set.empty
        let mutable server = null

        let rec loop () = actor {
            let! message = mailbox.Receive()
            let timeStamp = DateTime.Now
            match message with
            | InitUsersActor (actor1, actor2) ->
                showfeedActor <- actor1
                retweetActor <- actor2
                server <- mailbox.Sender()
            | RegisterUserWithUsersActor (userID', followersCount', time') ->
                // Get the amount of followers a user has and how many that user is following
                userFollowersCountMap <- Map.add userID' (followersCount' |> int) userFollowersCountMap
                userUsersFollowingSetMap <- Map.add userID' Set.empty userUsersFollowingSetMap
                followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
                serviceCount <- serviceCount + 1
            | Follow (clientID', userID', userToFollowID', time') ->
                // Follow another account as long as it is not the user themselves
                serviceCount <- serviceCount + 1
                if userUsersFollowingSetMap.ContainsKey userToFollowID' && not (userUsersFollowingSetMap.[userToFollowID'].Contains userToFollowID') && userUsersFollowingSetMap.[userToFollowID'].Count < userFollowersCountMap.[userToFollowID'] then
                    let mutable tempUsersFollowingSet = userUsersFollowingSetMap.[userID']
                    tempUsersFollowingSet <- Set.add userToFollowID' tempUsersFollowingSet
                    userUsersFollowingSetMap <- Map.add userID' tempUsersFollowingSet userUsersFollowingSetMap

                    let message = "[" + timeStamp.ToString() + "][FOLLOW] User " + clientID' + " started following " + userToFollowID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
            | Unfollow (clientID', userID') ->
                serviceCount <- serviceCount + 1

                let tempUsersFollowingList = Set.fold (fun l se -> se::l) [] (userUsersFollowingSetMap.Item(userID'))
                if tempUsersFollowingList.Length > 0 then
                    let userToUnfollowID' = tempUsersFollowingList.[Random().Next(tempUsersFollowingList.Length)]

                    let message = "[" + DateTime.Now.ToString() + "][UNFOLLOW] User " + clientID' + " unfollowed " + userToUnfollowID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
            | UpdateFeeds (clientID', userID', tweet', type', time') ->
                serviceCount <- serviceCount + 1
                for clientID in userUsersFollowingSetMap.[userID'] do
                    showfeedActor <! UpdateFeedTable(userID', tweet')
                    retweetActor <! UpdateRetweetFeedTable(userID', tweet')
                    if not (offlineUserIDsSet.Contains clientID') then
                        let userIDSplit = clientID.Split '_'
                        let clientIDToSendTo = userIDSplit.[0]
                        if type' = "tweet" then
                            let message = "[" + time'.ToString() + "][FEED_TWEET] For User: " + clientID + " -> " + tweet'
                            server <! ("ClientPrint", clientIDToSendTo, userID', message, DateTime.Now)
                        else // retweet
                            let message = "[" + time'.ToString() + "][FEED_RETWEET] For User: " + clientID + " -> " + userID' + ": " + tweet'
                            server <! ("ClientPrint", clientIDToSendTo, userID', message, DateTime.Now)
                followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
            | UsersActorGoOnline (clientID', userID', cAdmin', time') ->
                    serviceCount <- serviceCount + 1
                    offlineUserIDsSet <- Set.remove userID' offlineUserIDsSet
                    showfeedActor <! ShowFeeds(clientID', userID', cAdmin', mailbox.Sender())
                    followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds
            | UsersActorGoOffline (clientID', userID', time') ->
                    serviceCount <- serviceCount + 1
                    offlineUserIDsSet <- Set.add userID' offlineUserIDsSet
                    followTime <- followTime + (timeStamp.Subtract time').TotalMilliseconds

                    let message = "\n\n\n\n[" + time'.ToString() + "][OFFLINE] User " + userID' + " is going offline\n\n\n\n"
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
            (*| UsersPrint(stats, perf, reqTime) ->  // TODO
            serviceCount <- serviceCount + 1.0
            tweetactor <! PrintTweetStats(followings,stats,perf)
            followTime <- followTime + (timestamp.Subtract reqTime).TotalMilliseconds 

        let averageTime = followTime / serviceCount
        mailbox.Sender() <! ("ServiceStats","","Follow/Offline/Online",(averageTime |> string),DateTime.Now) *)
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
            | IDAndTweet (clientID', userID', tweet')->
                tweetCount <- tweetCount + 1
                let mutable tweetCounter = 0

                if userTweetCountMap.ContainsKey userID' then 
                    tweetCounter <- userTweetCountMap.[userID'] + 1
                userTweetCountMap <- Map.add userID' tweetCounter userTweetCountMap

                let message = "[" + DateTime.Now.ToString() + "][TWEET] " + tweet'
                mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                
                // These are performance metrics //TODO
                //twTotalTime <- twTotalTime + (timestamp.Subtract reqTime).TotalMilliseconds
                //let averageTime = twTotalTime / tweetCount
                //boss <! ("ServiceStats","","Tweet",(averageTime |> string),DateTime.Now)
            | IncrementTweetCount(userID') ->
                if userTweetCountMap.ContainsKey userID' then 
                    let tweetCounter = (userTweetCountMap.[userID'] + 1)
                    userTweetCountMap <- Map.add userID' tweetCounter userTweetCountMap
            (*| PrintTweetStats(followings,reqStats,perf) -> // TODO
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
                        File.AppendAllText(path, stat)*)
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let MentionsActor (mailbox:Actor<_>) = 
        let mutable usersSet = Set.empty
        let mutable userMentionsListMap = Map.empty
        let mutable queryCount = 0

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | RegisterUserWithMentionsActor (clientID', userID') ->
                usersSet <- Set.add userID' usersSet
                userMentionsListMap <- Map.add userID' List.empty userMentionsListMap
            | Tweet(tweet') ->
                for word in tweet'.Split ' ' do
                    if word.[0] = '@' then
                        let user = word.[1..(word.Length-1)]
                        if usersSet.Contains user then
                            let mutable mentionsList = userMentionsListMap.[user]
                            mentionsList <- tweet' :: mentionsList
                            userMentionsListMap <- Map.add user mentionsList userMentionsListMap
            | QueryMention(clientID', userID', mentionedUserID', time') ->
                queryCount <- queryCount + 1
                if userMentionsListMap.ContainsKey mentionedUserID' then
                    let mutable mentionSize = 10
                    if (userMentionsListMap.[mentionedUserID'].Length < 10) then
                        mentionSize <- userMentionsListMap.[mentionedUserID'].Length
                    let mutable tweetsString = ""
                    for i in [0..(mentionSize-1)] do
                        tweetsString <- tweetsString + "\n" + userMentionsListMap.[mentionedUserID'].[i]
                    let message = "[" + time'.ToString() + "][QUERY_MENTION] by user " + userID' + ": Recent 10(Max) tweets for user @" + mentionedUserID' + " -> " + tweetsString // TODO fix
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                else
                    let message = "[" + time'.ToString() + "][QUERY_MENTION] by user " + userID' + ": No tweets for user @" + mentionedUserID'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                //queryTotalTime <- queryTotalTime + (timestamp.Subtract reqTime).TotalMilliseconds // TODO
                //let averageTime = queryTotalTime / queryCount
                //mailbox.Sender() <! ("ServiceStats","","QueryMentions",(averageTime |> string),DateTime.Now)
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let HashtagsActor (mailbox:Actor<_>) = 
        let mutable hashtagTweetsListMap = Map.empty
        let mutable queryCount = 0

        let rec loop () = actor {
            let! message = mailbox.Receive() 
            match message with
            | Tweet(tweet') ->
                for word in tweet'.Split ' ' do
                    if word.[0] = '#' then
                        let hashtag = word.[1..(word.Length-1)]
                        if not (hashtagTweetsListMap.ContainsKey hashtag) then
                             hashtagTweetsListMap <- Map.add hashtag List.empty hashtagTweetsListMap

                        let mutable tweetsList = hashtagTweetsListMap.[word.[1..(word.Length-1)]]
                        tweetsList <- tweet' :: tweetsList
                        hashtagTweetsListMap <- Map.add hashtag tweetsList hashtagTweetsListMap
            | QueryHashtag (clientID', userID', hashtag', time') ->
                queryCount <- queryCount + 1
                if hashtagTweetsListMap.ContainsKey hashtag' then
                    let mutable hashtagSize = 10
                    if (hashtagTweetsListMap.[hashtag'].Length < 10) then
                        hashtagSize <- hashtagTweetsListMap.[hashtag'].Length
                    let mutable tagsstring = ""
                    for i in [0..(hashtagSize-1)] do
                        tagsstring <- tagsstring + "\n" + hashtagTweetsListMap.[hashtag'].[i]

                    let message = "[" + time'.ToString() + "][QUERY_HASHTAG] by user " + userID' + ": Recent 10(Max) tweets for hashtag #" + hashtag' + " -> " + tagsstring // TODO fix
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                else
                    let message = "[" + time'.ToString() + "][QUERY_HASHTAG] by user " + userID' + ": No tweets for hashtag #" + hashtag'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                
                //queryHTTotalTime <- queryHTTotalTime + (timestamp.Subtract reqTime).TotalMilliseconds // TODO
                //let averageHTTime = (queryHTTotalTime / queryHTCount)
                // printfn "cnt %f, totaltime %f, avg %f" queryHTCount queryHTTotalTime averageHTTime
                //mailbox.Sender() <! ("ServiceStats","","QueryHashTag",(averageHTTime |> string),DateTime.Now)
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
            | Retweet(clientID', userID', time') ->
                if userIDFeedListMap.ContainsKey userID' then
                    retweetCount <- retweetCount + 1
                    let randTweet = userIDFeedListMap.[userID'].[Random().Next(userIDFeedListMap.[userID'].Length)]
                    let message = "[" + time'.ToString() + "][RETWEET] by user " + userID' + ": " + randTweet
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                    
                    //reTweetTime <- reTweetTime + (timeStamp.Subtract time').TotalMilliseconds // TODO
                    //let averageTime = reTweetTime / reTweetCount
                    //mailbox.Sender() <! ("ServiceStats","","ReTweet",(averageTime |> string),DateTime.Now)
                    usersActor <! UpdateFeeds(clientID', userID', randTweet, "retweet", DateTime.Now)
                    tweetActor <! IncrementTweetCount(userID') // Increment user's tweet count
            return! loop()
        }
        loop()

    let ShowFeedActor (mailbox:Actor<_>) = 
        let mutable userIDFeedListMap = Map.empty

        let rec loop () = actor {
            let! message = mailbox.Receive()
            let timeStamp = DateTime.Now
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

                    let message = "[" + timeStamp.ToString() + "][ONLINE] User " + userID' + " is online..Feeds -> " + feedsTop
                    server' <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                else
                    let message = "[" + timeStamp.ToString() + "][ONLINE] User " + userID' + " is online..No feeds yet!!!"
                    server' <! ("ClientPrint", clientID', userID', message, DateTime.Now)
                cAdmin' <! ("AckOnline", userID', "", "", "")
            | UpdateFeedTable (userID', tweet') ->
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
        let mutable stats = Map.empty
        let mutable start = DateTime.Now

        let rec loop () = actor {
            let! (message:obj) = mailbox.Receive()
            let timestamp = DateTime.Now
            let (messageType, p1, p2, p3, time) : Tuple<string,string,string,string,DateTime> = downcast message
            match messageType with
            | "StartServer" ->
                printfn "Server Started"
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(3000.0), mailbox.Self, ("StatsPrinter","","","",DateTime.Now))
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
                let clientPrinterActorSelection = system.ActorSelection(sprintf "akka.tcp://Client@%s:%s/user/Printer" p2 p3)
                clientIDClientPrinterMap <- Map.add p1 clientPrinterActorSelection clientIDClientPrinterMap
                let message = "[" + timestamp.ToString() + "][CLIENT_REGISTER] Client " + p1 + " registered with server"
                mailbox.Sender() <! ("AckClientReg", message, "", "", "")
            | "UserRegistration" -> // clientID, userID, followers, DateTime.Now)
                usersActor <! RegisterUserWithUsersActor(p2, p3, time)
                mentionsActor <! RegisterUserWithMentionsActor(p1, p2)
                requestsCount <- requestsCount + 1UL
                let message = "[" + timestamp.ToString() + "][USER_REGISTER] User " + p2 + " registered with server"
                mailbox.Sender() <! ("AckUserReg", p2, message, "", "")
            | "Tweet" ->
                requestsCount <- requestsCount + 1UL
                mentionsActor <! Tweet(p3) // forward tweet to mentions Actor
                hashtagsActor <! Tweet(p3) // forward tweet to hashtags Actor
                tweetsActor <! IDAndTweet(p1, p2, p3) // forward tweet to tweets Actor
                usersActor <! UpdateFeeds(p1, p2, p3, "tweet", DateTime.Now)
            | "Retweet"  ->
                requestsCount <- requestsCount + 1UL
                retweetsActor <! Retweet(p1, p2, time) // forward tweet to retweets Actor
            | "Follow" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! Follow(p1, p2, p3, time) // forward to users Actor
            | "Unfollow" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! Unfollow(p1, p2) // forward tweet to users Actor
            | "ServerGoOffline" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOffline(p1, p2, time)
            | "ServerGoOnline" ->
                requestsCount <- requestsCount + 1UL
                usersActor <! UsersActorGoOnline(p1, p2, mailbox.Sender(), time)
            | "QueryHashtag" ->
                requestsCount <- requestsCount + 1UL
                hashtagsActor <! QueryHashtag(p1, p2, p3, time)
            | "QueryMention" ->
                requestsCount <- requestsCount + 1UL
                mentionsActor <! QueryMention(p1, p2, p3, time)
            | "ClientPrint" ->
                requestsCount <- requestsCount + 1UL
                clientIDClientPrinterMap.[p1] <! p3
            | "DisplayTotalStats" ->
                // Update stats for specific parameter
                if stats.ContainsKey p2 then
                     stats <- Map.remove p2 stats
                stats <- Map.add p2 p3 stats

                 //printfn "Here we have a %A that has been up for %A and has been looked at %A" p2 p3 time
                 //printfn "Displaying server stats"
            | "StatsPrinter" ->
                 let mutable temp = 0UL
                 temp <- requestsCount / ((DateTime.Now.Subtract start).TotalSeconds |> uint64)
                 printfn "Server uptime = %u seconds, requests served = %u, Avg requests served = %u per second" ((DateTime.Now.Subtract start).TotalSeconds |> uint64) requestsCount temp
                 system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(3000.0), mailbox.Self, ("StatsPrinter","","","",DateTime.Now))
            | _ ->
                ignore()
            return! loop()
        }
        loop()

    let boss = spawn system "ServerActor" ServerActor // Start - spawn boss

    boss <! ("StartServer", "", "", "", DateTime.Now)

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
                        server <! ("Follow", clientID, userID, userToFollowID, DateTime.Now)
                    | 2 ->  // unfollow
                        server <! ("Unfollow", clientID, userID, "", DateTime.Now)
                    | 3 ->  // query hashtag
                        let hashTag = hashtagsList.[Random().Next(hashtagsList.Length)]
                        server <! ("QueryHashtag", clientID, userID, hashTag, DateTime.Now)
                    | 4 ->  // query mention
                        let mutable mUser = [1 .. usersCount].[Random().Next(usersCount)] |> string
                        let mutable randclientID = clientsList.[Random().Next(clientsList.Length)]
                        let mutable mentionedUserID = randclientID + "_" + mUser
                        server <! ("QueryMention", clientID, userID, mentionedUserID, DateTime.Now)
                    | _ ->
                        ()
                    system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, StartOtherAction)
            | UserActorGoOffline ->
                online <- false
            | UserActorGoOnline ->
                online <- true
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(100.0), mailbox.Self, Tweet)
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromMilliseconds(101.0), mailbox.Self, StartOtherAction)
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
        "NASB"; "Ticketmaster"; "gowon"; "Stacy"; "Garnet"; "Gaetz"; "Accused"; "Omarova"; "Cenk"; "McMuffin";]

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
                let shuffle a =
                    Array.iteri (fun i _ -> swap a i (Random().Next(i, Array.length a))) a

                shuffle usersArray
                usersList <- usersArray |> Array.toList
                //printfn "second users array: "
                //for i in 1..int32(p2)-1 do
                //    printfn "val: %s" (string(usersArray.[i]))
                for i in [1 .. (p2 |> int32)] do
                    let userKey = usersArray.[i-1] |> string
                    userFollowersRankMap <- Map.add (clientID + "_" + userKey) ((int32(p2)-1)/i) userFollowersRankMap
                    userIntervalMap <- Map.add (clientID  + "_" + userKey) i userIntervalMap
                server <! ("ClientRegistration", clientID, clientIP, clientPort, DateTime.Now)
                for i in [1 .. int32(p3)] do
                    clientsList <- (i |> string) :: clientsList
            | "AckClientReg" ->
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
                    
                    server <! ("ServerGoOffline", clientID, nextOfflineUserID, "", DateTime.Now)
                    userIDUserActorRefMap.[nextOfflineUserID] <! UserActorGoOffline
                    tempOfflineUserIDsSet <- Set.add nextOfflineUserID tempOfflineUserIDsSet

                for ID in currentlyOfflineUsersSet do
                    server <! ("ServerGoOnline", clientID, ID, "", DateTime.Now)
                    
                currentlyOfflineUsersSet <- tempOfflineUserIDsSet
                system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(5.0), mailbox.Self, ("ClientActorGoOffline", "", "", "", ""))

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
