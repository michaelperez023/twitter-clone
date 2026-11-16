// Twitter clone: a Twitter-like engine and a client-side load simulator built with Akka.NET actors.
// COP 5615 Distributed Operating System Principles, Project 4 Part I (University of Florida, Fall 2021).
// Run `dotnet fsi TwitterClone.fsx help` for usage, and see README.md for details.

#r "nuget: Akka, 1.5.74"
#r "nuget: Akka.Remote, 1.5.74"
#r "nuget: Akka.FSharp, 1.5.74"

open System
open System.Collections.Generic
open System.Diagnostics
open System.Globalization
open Akka.Actor
open Akka.FSharp
open Akka.Configuration

// ---------------------------------------------------------------------------------------------
// Messages between actors in the same process. Only ServerActor, AdminActor and Printer get messages
// from other processes: tuples of strings for the first two, and plain strings for Printer.
// ---------------------------------------------------------------------------------------------

type UserActorMessages =
    | StartUserActor of userNumber: int * tweetInterval: TimeSpan * server: IActorRef
    | StartTweet of session: int
    | StartOtherAction of session: int
    | UserActorGoOffline
    | UserActorGoOnline

type UsersActorMessages =
    | RegisterUserWithUsersActor of userID: string * followersCap: int
    | Follow of clientID: string * userID: string * userToFollowID: string
    | Unfollow of clientID: string * userID: string
    | UsersActorGoOnline of clientID: string * userID: string * clientActor: IActorRef
    | UsersActorGoOffline of clientID: string * userID: string
    | UpdateFeeds of clientID: string * userID: string * tweet: string * kind: string

type RetweetsActorMessages =
    | UpdateRetweetFeedTable of userID: string * tweet: string
    | Retweet of clientID: string * userID: string

type FeedActorMessages =
    // Store a tweet in a user's feed, and show liveMessage to the user unless it is empty (the user is offline)
    | AddToFeed of clientID: string * userID: string * tweet: string * liveMessage: string
    | ShowFeeds of clientID: string * userID: string * clientActor: IActorRef
    | Relay of clientID: string * userID: string * message: string

type TweetsHashtagsMentionsActorsMessages =
    | IDAndTweet of clientID: string * userID: string * tweet: string
    | Tweet of tweet: string
    | RegisterUserWithMentionsActor of userID: string
    | QueryHashtag of clientID: string * userID: string * hashtag: string
    | QueryMention of clientID: string * userID: string * mentionedUserID: string

// ---------------------------------------------------------------------------------------------
// Command line and shared helpers
// ---------------------------------------------------------------------------------------------

let serverPort = 4800
let maxClients = 100
let maxUsersPerClient = 100000

// Each user's feed, each hashtag and each user's mentions keep this many of their most recent tweets
let maxStoredItems = 100

let usage =
    """Usage:
  dotnet fsi TwitterClone.fsx server <server_ip>
  dotnet fsi TwitterClone.fsx client <client_ip> <client_port> <client_id> <users_per_client> <clients> <server_ip>

The server listens on TCP port 4800. Start it first; a client that gets no answer asks again every 3 seconds.
Client IDs run from 1 to <clients> (at most 100), and every client must use the same <users_per_client>
(at most 100000) and <clients>.

Example with a server and two clients on one machine:
  dotnet fsi TwitterClone.fsx server 127.0.0.1
  dotnet fsi TwitterClone.fsx client 127.0.0.1 4801 1 500 2 127.0.0.1
  dotnet fsi TwitterClone.fsx client 127.0.0.1 4802 2 500 2 127.0.0.1"""

let args = fsi.CommandLineArgs |> Array.skip 1

let fail (message: string) : 'a =
    eprintfn "Error: %s\n\n%s" message usage
    exit 2

let parseInt (name: string) (text: string) (low: int) (high: int) =
    match Int32.TryParse text with
    | true, value when value >= low && value <= high -> value
    | _ -> fail (sprintf "<%s> must be a whole number from %d to %d (got '%s')" name low high text)

let toInt (text: string) =
    match Int32.TryParse text with
    | true, value -> value
    | _ -> 0

let remoteConfig (hostname: string) (port: int) =
    sprintf
        """
akka {
  actor.provider = remote
  remote.dot-netty.tcp {
    hostname = "%s"
    port = %d
  }
}"""
        hostname
        port
    |> ConfigurationFactory.ParseString

let timestamp () =
    DateTime.Now.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture)

/// The distinct words of a tweet that start with the marker ('#' or '@'), without the marker
let taggedWords (marker: char) (text: string) =
    text.Split(' ', StringSplitOptions.RemoveEmptyEntries)
    |> Array.filter (fun word -> word.Length > 1 && word.[0] = marker)
    |> Array.map (fun word -> word.Substring 1)
    |> Array.distinct

/// The newest items stored under each key, at most `capacity` per key; older items are dropped
type RecentItems(capacity: int) =
    let table = Dictionary<string, Queue<string>>()

    member _.Add(key: string, item: string) =
        let queue =
            match table.TryGetValue key with
            | true, queue -> queue
            | _ ->
                let queue = Queue<string>()
                table.[key] <- queue
                queue

        queue.Enqueue item

        if queue.Count > capacity then
            queue.Dequeue() |> ignore

    /// Up to n of the newest items under the key, newest first
    member _.Newest(key: string, n: int) =
        match table.TryGetValue key with
        | true, queue -> queue |> Seq.skip (max 0 (queue.Count - n)) |> Seq.rev |> List.ofSeq
        | _ -> []

    /// A random item under the key, or None if there is none
    member _.PickRandom(key: string) =
        match table.TryGetValue key with
        | true, queue when queue.Count > 0 -> Some(Seq.item (Random.Shared.Next queue.Count) queue)
        | _ -> None

let mode = if args.Length > 0 then args.[0].ToLowerInvariant() else ""

match mode with
| "help" ->
    printfn "%s" usage
    exit 0
| "server"
| "client" -> ()
| "" -> fail "missing mode: server or client"
| _ -> fail (sprintf "unknown mode '%s'" args.[0])

// ---------------------------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------------------------

if mode = "server" then
    if args.Length <> 2 then
        fail "server takes one argument: <server_ip>"

    let serverIP = args.[1]
    let system = ActorSystem.Create("Server", remoteConfig serverIP serverPort)

    // The client ID part of a user ID ("<client_id>_<user_number>")
    let clientOf (userID: string) = userID.Split('_').[0]

    let UsersActor (feedActor: IActorRef) (retweetsActor: IActorRef) (mailbox: Actor<_>) =
        let mutable followersCapMap = Map.empty // user ID -> maximum number of followers
        let mutable followersSetMap = Map.empty // user ID -> users who follow this user
        let mutable followingSetMap = Map.empty // user ID -> users this user follows
        let mutable offlineUserIDsSet = Set.empty

        let followersOf userID =
            defaultArg (Map.tryFind userID followersSetMap) Set.empty

        let followingOf userID =
            defaultArg (Map.tryFind userID followingSetMap) Set.empty

        // Output for a user goes through FeedActor, which also sends the user's feed and adds the
        // timestamps, so the user's client gets it in the order this actor produced it
        let tellUser clientID userID message = feedActor <! Relay(clientID, userID, message)

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | RegisterUserWithUsersActor(userID', followersCap') ->
                    // Store the user's follower cap and start with empty follower and following sets
                    followersCapMap <- Map.add userID' followersCap' followersCapMap
                    followersSetMap <- Map.add userID' Set.empty followersSetMap
                    followingSetMap <- Map.add userID' Set.empty followingSetMap
                | Follow(clientID', userID', userToFollowID') ->
                    // Follow another registered user, unless it is the same user, the user already follows it,
                    // or it already has as many followers as its cap allows
                    match Map.tryFind userToFollowID' followersCapMap with
                    | Some cap when userToFollowID' <> userID' && followingSetMap.ContainsKey userID' ->
                        let followers = followersOf userToFollowID'

                        if not (followers.Contains userID') && followers.Count < cap then
                            followersSetMap <- Map.add userToFollowID' (Set.add userID' followers) followersSetMap
                            followingSetMap <- Map.add userID' (Set.add userToFollowID' (followingOf userID')) followingSetMap

                            sprintf "[FOLLOW] user %s followed %s (%d of %d followers)" userID' userToFollowID' (followers.Count + 1) cap
                            |> tellUser clientID' userID'
                    | _ -> ()
                | Unfollow(clientID', userID') ->
                    // Unfollow a random user from the user's following set, if it is not empty
                    let following = followingOf userID'

                    if not following.IsEmpty then
                        let userToUnfollowID' = Seq.item (Random.Shared.Next following.Count) following
                        followingSetMap <- Map.add userID' (Set.remove userToUnfollowID' following) followingSetMap
                        followersSetMap <- Map.add userToUnfollowID' (Set.remove userID' (followersOf userToUnfollowID')) followersSetMap
                        sprintf "[UNFOLLOW] user %s unfollowed %s" userID' userToUnfollowID' |> tellUser clientID' userID'
                | UpdateFeeds(_, userID', tweet', kind') ->
                    // Add the tweet or retweet to the feed of every follower of the user, and show it right away
                    // to the followers who are online
                    for followerID in followersOf userID' do
                        let liveMessage =
                            if offlineUserIDsSet.Contains followerID then ""
                            elif kind' = "retweet" then sprintf "[FEED_RETWEET] for user %s: %s retweeted '%s'" followerID userID' tweet'
                            else sprintf "[FEED_TWEET] for user %s: '%s'" followerID tweet'

                        feedActor <! AddToFeed(clientOf followerID, followerID, tweet', liveMessage)
                        retweetsActor <! UpdateRetweetFeedTable(followerID, tweet')

                    // Show a new tweet right away to the registered users it mentions who are online
                    if kind' = "tweet" then
                        for mentionedID in taggedWords '@' tweet' do
                            if mentionedID <> userID' && followersCapMap.ContainsKey mentionedID && not (offlineUserIDsSet.Contains mentionedID) then
                                sprintf "[MENTION] for user %s: '%s'" mentionedID tweet' |> tellUser (clientOf mentionedID) mentionedID
                | UsersActorGoOnline(clientID', userID', clientActor') ->
                    // Mark the user online again and show them their feed
                    offlineUserIDsSet <- Set.remove userID' offlineUserIDsSet
                    feedActor <! ShowFeeds(clientID', userID', clientActor')
                | UsersActorGoOffline(clientID', userID') ->
                    // Mark the user offline; new tweets still go into their feed but are not shown live
                    offlineUserIDsSet <- Set.add userID' offlineUserIDsSet
                    sprintf "[OFFLINE] User %s is going offline" userID' |> tellUser clientID' userID'

                return! loop ()
            }

        loop ()

    let TweetsActor (mailbox: Actor<_>) =
        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | IDAndTweet(clientID', userID', tweet') ->
                    // Send the tweet back to the author's client to be printed
                    let message = sprintf "[%s] [TWEET] %s" (timestamp ()) tweet'
                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                | _ -> ()

                return! loop ()
            }

        loop ()

    let MentionsActor (mailbox: Actor<_>) =
        let usersSet = HashSet<string>()
        let mentions = RecentItems(maxStoredItems) // user ID -> recent tweets that mention the user

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | RegisterUserWithMentionsActor(userID') -> usersSet.Add userID' |> ignore
                | Tweet(tweet') ->
                    // Index the tweet under every registered user it mentions with @
                    for mentionedUserID in taggedWords '@' tweet' do
                        if usersSet.Contains mentionedUserID then
                            mentions.Add(mentionedUserID, tweet')
                | QueryMention(clientID', userID', mentionedUserID') ->
                    // Reply with up to 10 of the most recent tweets that mention the user
                    let message =
                        match mentions.Newest(mentionedUserID', 10) with
                        | [] -> sprintf "[%s] [QUERY_MENTION] by user %s - no tweets mention @%s" (timestamp ()) userID' mentionedUserID'
                        | recent -> sprintf "[%s] [QUERY_MENTION] by user %s - %d most recent tweets that mention @%s:\n%s" (timestamp ()) userID' recent.Length mentionedUserID' (String.Join("\n", recent))

                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                | _ -> ()

                return! loop ()
            }

        loop ()

    let HashtagsActor (mailbox: Actor<_>) =
        let hashtagTweets = RecentItems(maxStoredItems) // hashtag -> recent tweets with the hashtag

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | Tweet(tweet') ->
                    // Index the tweet under every hashtag it contains
                    for hashtag in taggedWords '#' tweet' do
                        hashtagTweets.Add(hashtag, tweet')
                | QueryHashtag(clientID', userID', hashtag') ->
                    // Reply with up to 10 of the most recent tweets that have the hashtag
                    let message =
                        match hashtagTweets.Newest(hashtag', 10) with
                        | [] -> sprintf "[%s] [QUERY_HASHTAG] by user %s - no tweets have #%s" (timestamp ()) userID' hashtag'
                        | recent -> sprintf "[%s] [QUERY_HASHTAG] by user %s - %d most recent tweets with #%s:\n%s" (timestamp ()) userID' recent.Length hashtag' (String.Join("\n", recent))

                    mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                | _ -> ()

                return! loop ()
            }

        loop ()

    let RetweetsActor (mailbox: Actor<_>) =
        let feeds = RecentItems(maxStoredItems) // user ID -> recent tweets in the user's feed
        let usersActor = select "../UsersActor" mailbox

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | UpdateRetweetFeedTable(userID', tweet') ->
                    // Keep a copy of the user's feed to pick retweets from
                    feeds.Add(userID', tweet')
                | Retweet(clientID', userID') ->
                    // Retweet a random recent tweet from the user's feed and send it to the user's followers
                    match feeds.PickRandom userID' with
                    | Some randTweet ->
                        let message = sprintf "[%s] [RETWEET] by user %s: %s" (timestamp ()) userID' randTweet
                        mailbox.Sender() <! ("ClientPrint", clientID', userID', message)
                        usersActor <! UpdateFeeds(clientID', userID', randTweet, "retweet")
                    | None -> ()

                return! loop ()
            }

        loop ()

    let FeedActor (mailbox: Actor<_>) =
        let feeds = RecentItems(maxStoredItems) // user ID -> recent tweets in the user's feed
        let server = mailbox.Context.Parent

        // Send a line, with a timestamp, to the user's client through ServerActor
        let show clientID userID (message: string) =
            server <! ("ClientPrint", clientID, userID, sprintf "[%s] %s" (timestamp ()) message)

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | AddToFeed(clientID', userID', tweet', liveMessage') ->
                    feeds.Add(userID', tweet')

                    if liveMessage' <> "" then
                        show clientID' userID' liveMessage'
                | ShowFeeds(clientID', userID', clientActor') ->
                    // Show the user up to 10 of the most recent tweets in their feed, then tell the client
                    // that the user is back online
                    let message =
                        match feeds.Newest(userID', 10) with
                        | [] -> sprintf "[ONLINE] User %s is online, no tweets in feed yet" userID'
                        | recent -> sprintf "[ONLINE] User %s is online, %d most recent tweets in feed:\n%s" userID' recent.Length (String.Join("\n", recent))

                    show clientID' userID' message
                    clientActor' <! ("ClientGoOnline", userID', "", "", "")
                | Relay(clientID', userID', message') -> show clientID' userID' message'

                return! loop ()
            }

        loop ()

    let ServerActor (mailbox: Actor<obj>) =
        // The workers are children of ServerActor, which restarts a worker if it fails. A restarted worker
        // loses the data it held but keeps its references to the other workers.
        let feedActor = spawn mailbox "FeedActor" FeedActor
        let retweetsActor = spawn mailbox "RetweetsActor" RetweetsActor
        let usersActor = spawn mailbox "UsersActor" (UsersActor feedActor retweetsActor)
        let tweetsActor = spawn mailbox "TweetsActor" TweetsActor
        let hashtagsActor = spawn mailbox "HashtagsActor" HashtagsActor
        let mentionsActor = spawn mailbox "MentionsActor" MentionsActor

        // Client ID -> the client's Printer actor: an actor path at first, and a direct reference once
        // the Printer has answered an Identify message
        let clientPrinters = Dictionary<string, ICanTell>()

        // Throughput counters. Only requests sent by clients count, not the output the server relays
        // back to clients.
        let requestTypes =
            [| "ClientRegistration"; "UserRegistration"; "Tweet"; "Retweet"; "Follow"; "Unfollow"
               "QueryHashtag"; "QueryMention"; "ServerGoOffline"; "ServerGoOnline" |]

        let requestCounts = Dictionary<string, int64>()

        for requestType in requestTypes do
            requestCounts.[requestType] <- 0L

        let mutable requestsCount = 0L
        let mutable requestsAtLastStats = 0L
        let clock = Stopwatch.StartNew()
        let mutable lastStatsAt = TimeSpan.Zero
        let mutable firstRequestAt = TimeSpan.Zero

        let printStats () =
            let now = clock.Elapsed
            let interval = (now - lastStatsAt).TotalSeconds
            let recentRate = float (requestsCount - requestsAtLastStats) / interval
            let activeSeconds = (now - firstRequestAt).TotalSeconds

            let averageRate =
                if requestsCount > 0L && activeSeconds > 0.0 then
                    float requestsCount / activeSeconds
                else
                    0.0

            printfn "[STATS] uptime %.0f s | client requests: %d | last %.1f s: %.0f req/s | average since first request: %.0f req/s" now.TotalSeconds requestsCount interval recentRate averageRate
            printfn "[STATS] by type: %s" (requestTypes |> Array.map (fun t -> sprintf "%s=%d" t requestCounts.[t]) |> String.concat " ")
            lastStatsAt <- now
            requestsAtLastStats <- requestsCount

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | :? (string * string * string * string) as request ->
                    let (messageType, p1, p2, p3) = request

                    if requestCounts.ContainsKey messageType then
                        if requestsCount = 0L then
                            firstRequestAt <- clock.Elapsed

                        requestsCount <- requestsCount + 1L
                        requestCounts.[messageType] <- requestCounts.[messageType] + 1L

                    match messageType with
                    | "ClientRegistration" -> // p1, p2, p3 = clientID, clientIP, clientPort
                        let clientPrinter = system.ActorSelection(sprintf "akka.tcp://Client@%s:%s/user/Printer" p2 p3)
                        clientPrinters.[p1] <- clientPrinter :> ICanTell
                        clientPrinter <! Identify(p1)
                        let message = sprintf "[%s] [CLIENT_REGISTER] Client %s registered with server" (timestamp ()) p1
                        mailbox.Sender() <! ("ACKClientRegistration", message, "", "", "")
                    | "UserRegistration" -> // p1, p2, p3 = clientID, userID, followers cap
                        let followersCap = toInt p3
                        usersActor <! RegisterUserWithUsersActor(p2, followersCap)
                        mentionsActor <! RegisterUserWithMentionsActor(p2)
                        let message = sprintf "[%s] [USER_REGISTER] User %s registered with server (follower cap %d)" (timestamp ()) p2 followersCap
                        mailbox.Sender() <! ("ACKUserRegistration", p2, message, "", "")
                    | "Tweet" -> // p1, p2, p3 = clientID, userID, tweet
                        mentionsActor <! Tweet(p3)
                        hashtagsActor <! Tweet(p3)
                        tweetsActor <! IDAndTweet(p1, p2, p3)
                        usersActor <! UpdateFeeds(p1, p2, p3, "tweet")
                    | "Retweet" -> // p1, p2 = clientID, userID
                        retweetsActor <! Retweet(p1, p2)
                    | "Follow" -> // p1, p2, p3 = clientID, userID, user to follow
                        usersActor <! Follow(p1, p2, p3)
                    | "Unfollow" -> // p1, p2 = clientID, userID
                        usersActor <! Unfollow(p1, p2)
                    | "ServerGoOffline" -> // p1, p2 = clientID, userID
                        usersActor <! UsersActorGoOffline(p1, p2)
                    | "ServerGoOnline" -> // p1, p2 = clientID, userID
                        usersActor <! UsersActorGoOnline(p1, p2, mailbox.Sender())
                    | "QueryHashtag" -> // p1, p2, p3 = clientID, userID, hashtag
                        hashtagsActor <! QueryHashtag(p1, p2, p3)
                    | "QueryMention" -> // p1, p2, p3 = clientID, userID, mentioned user
                        mentionsActor <! QueryMention(p1, p2, p3)
                    | "ClientPrint" -> // p1, p3 = clientID, text to print on that client
                        match clientPrinters.TryGetValue p1 with
                        | true, clientPrinter -> clientPrinter.Tell(p3, ActorRefs.NoSender)
                        | _ -> ()
                    | "StatsPrinter" -> printStats ()
                    | _ -> ()
                | :? ActorIdentity as identity ->
                    // A client's Printer answered: send its output straight to it from now on
                    match identity.MessageId with
                    | :? string as clientID when not (isNull identity.Subject) -> clientPrinters.[clientID] <- identity.Subject :> ICanTell
                    | _ -> ()
                | _ -> mailbox.Unhandled message

                return! loop ()
            }

        loop ()

    let boss = spawn system "ServerActor" ServerActor
    system.Scheduler.ScheduleTellRepeatedly(TimeSpan.FromSeconds(3.), TimeSpan.FromSeconds(3.), boss, ("StatsPrinter", "", "", ""), ActorRefs.NoSender)
    printfn "Server started on %s:%d" serverIP serverPort
    system.WhenTerminated.Wait()

// ---------------------------------------------------------------------------------------------
// Client: simulates <users_per_client> users
// ---------------------------------------------------------------------------------------------

if mode = "client" then
    if args.Length <> 7 then
        fail "client takes six arguments: <client_ip> <client_port> <client_id> <users_per_client> <clients> <server_ip>"

    let clientIP = args.[1]
    let clientPort = parseInt "client_port" args.[2] 1 65535
    let clientsCount = parseInt "clients" args.[5] 1 maxClients
    let clientNumber = parseInt "client_id" args.[3] 1 clientsCount
    let usersCount = parseInt "users_per_client" args.[4] 1 maxUsersPerClient
    let serverIP = args.[6]

    if clientIP = serverIP && clientPort = serverPort then
        fail (sprintf "<client_port> must not be the server port %d when the client and server share an IP address" serverPort)

    let clientID = string clientNumber
    let system = ActorSystem.Create("Client", remoteConfig clientIP clientPort)
    let serverSelection = system.ActorSelection(sprintf "akka.tcp://Server@%s:%d/user/ServerActor" serverIP serverPort)

    let hashtags =
        [| "COP5615isgreat"; "FSharp"; "DotNet"; "Akka"; "ActorModel"; "DistributedSystems"; "OpenSource"; "Coding"
           "Gators"; "Gainesville"; "Florida"; "Coffee"; "Breakfast"; "Pizza"; "Tacos"; "Cooking"
           "Weekend"; "MondayMotivation"; "ThrowbackThursday"; "FridayFeeling"; "Music"; "Movies"; "Books"; "Podcasts"
           "Travel"; "Photography"; "Sunset"; "Beach"; "Hiking"; "Running"; "Fitness"; "Soccer"
           "Basketball"; "Baseball"; "Gaming"; "Space"; "Science"; "Gardening"; "Dogs"; "Cats" |]

    let randomHashtag () =
        hashtags.[Random.Shared.Next hashtags.Length]

    // User IDs are "<client_id>_<user_number>", with user numbers from 1 to <users_per_client> on every client
    let randomUserID () =
        sprintf "%d_%d" (Random.Shared.Next(1, clientsCount + 1)) (Random.Shared.Next(1, usersCount + 1))

    // A random user other than the given one on this client, or None if the simulation has only one user
    let randomOtherUserID (userNumber: int) =
        let total = clientsCount * usersCount

        if total < 2 then
            None
        else
            let self = (clientNumber - 1) * usersCount + (userNumber - 1)
            let pick = Random.Shared.Next(total - 1)
            let index = if pick >= self then pick + 1 else pick
            Some(sprintf "%d_%d" (index / usersCount + 1) (index % usersCount + 1))

    // Each client ranks its users from 1 to <users_per_client> by shuffling the user numbers with its
    // client ID as the seed. rankTables.[c - 1].[r - 1] is the number of the user with rank r on client c,
    // so every client can find the user with a given rank on any client.
    let rankTables =
        Array.init clientsCount (fun i -> lazy (Array.randomShuffleWith (Random(i + 1)) [| 1..usersCount |]))

    // Cumulative probabilities of the ranks when rank r is picked with probability proportional to 1 / r
    let zipfCdf =
        let weights = Array.init usersCount (fun i -> 1.0 / float (i + 1))
        let total = Array.sum weights
        let cdf = Array.scan (+) 0.0 weights |> Array.tail |> Array.map (fun sum -> sum / total)
        cdf.[usersCount - 1] <- 1.0
        cdf

    let randomZipfRank () =
        let index =
            match System.Array.BinarySearch(zipfCdf, Random.Shared.NextDouble()) with
            | i when i >= 0 -> i + 1
            | i -> ~~~i

        index + 1

    // A user to follow, other than the given one on this client: a random client, then a rank picked with
    // probability proportional to 1 / rank, so top-ranked users get the most followers.
    // None if the simulation has only one user.
    let randomFollowTarget (userNumber: int) =
        if clientsCount * usersCount < 2 then
            None
        else
            let rec pick () =
                let client = Random.Shared.Next(1, clientsCount + 1)
                let target = rankTables.[client - 1].Force().[randomZipfRank () - 1]

                if client = clientNumber && target = userNumber then
                    pick ()
                else
                    Some(sprintf "%d_%d" client target)

            pick ()

    let PrinterActor (mailbox: Actor<string>) =
        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                if not (isNull message) then
                    Console.WriteLine message

                return! loop ()
            }

        loop ()

    let printerRef = spawn system "Printer" PrinterActor

    let UserActor (mailbox: Actor<UserActorMessages>) =
        let mutable online = false
        // Increases every time the user comes online. Timer messages carry the session they were
        // scheduled in, so the tweet and action loops of an earlier session stop instead of doubling up.
        let mutable session = 0
        let mutable userID = ""
        let mutable userNumber = 0
        let mutable tweetInterval = TimeSpan.Zero
        let mutable tweetCount = 0
        let mutable server: IActorRef = ActorRefs.Nobody

        let schedule (delay: TimeSpan) (message: UserActorMessages) =
            system.Scheduler.ScheduleTellOnce(delay, mailbox.Self, message)

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | StartUserActor(userNumber', tweetInterval', server') ->
                    userNumber <- userNumber'
                    userID <- clientID + "_" + string userNumber'
                    tweetInterval <- tweetInterval'
                    server <- server'
                    online <- true
                    session <- session + 1
                    schedule tweetInterval (StartTweet session)
                    schedule (TimeSpan.FromSeconds(0.02)) (StartOtherAction session)
                | StartTweet s when online && s = session ->
                    // Post a random kind of tweet: plain, hashtag and mention, hashtag, mention, or a retweet
                    match Random.Shared.Next(1, 6) with
                    | 5 -> server <! ("Retweet", clientID, userID, "")
                    | kind ->
                        tweetCount <- tweetCount + 1
                        let text = sprintf "user %s tweeted tweet%d" userID tweetCount

                        let text =
                            match kind, randomOtherUserID userNumber with
                            | 2, Some other -> sprintf "%s with hashtag #%s and mention @%s" text (randomHashtag ()) other
                            | 3, _ -> sprintf "%s with hashtag #%s" text (randomHashtag ())
                            | 4, Some other -> sprintf "%s with mention @%s" text other
                            | _ -> text

                        server <! ("Tweet", clientID, userID, text)

                    schedule tweetInterval (StartTweet s)
                | StartOtherAction s when online && s = session ->
                    // Every 0.2 s: query a hashtag, unfollow, follow, or query a user's mentions
                    match Random.Shared.Next(1, 5) with
                    | 1 -> server <! ("QueryHashtag", clientID, userID, randomHashtag ())
                    | 2 -> server <! ("Unfollow", clientID, userID, "")
                    | 3 ->
                        match randomFollowTarget userNumber with
                        | Some other -> server <! ("Follow", clientID, userID, other)
                        | None -> ()
                    | _ -> server <! ("QueryMention", clientID, userID, randomUserID ())

                    schedule (TimeSpan.FromSeconds(0.2)) (StartOtherAction s)
                | StartTweet _
                | StartOtherAction _ -> () // the user is offline, or the timer is from an earlier session
                | UserActorGoOnline ->
                    online <- true
                    session <- session + 1
                    schedule (TimeSpan.FromSeconds(0.1)) (StartTweet session)
                    schedule (TimeSpan.FromSeconds(0.2)) (StartOtherAction session)
                | UserActorGoOffline -> online <- false

                return! loop ()
            }

        loop ()

    let ClientActor (mailbox: Actor<obj>) =
        // Users are registered in rank order. The user with rank r may have up to
        // (users_per_client - 1) / r followers and tweets every baseTweetInterval * r milliseconds, and
        // follow requests pick rank r with probability proportional to 1 / r (see randomFollowTarget).
        let userNumbersByRank = rankTables.[clientNumber - 1].Force()
        let baseTweetInterval = max 10 (usersCount / 100)
        let mutable server: IActorRef = null // the server's ServerActor, once it has answered
        let mutable userActorRefMap = Map.empty // user ID -> (actor, user number, rank)
        let registeredUserIDs = ResizeArray<string>()
        let mutable currentlyOfflineUsersSet = Set.empty

        let scheduleSelf (seconds: float) (message: string * string * string * string * string) =
            system.Scheduler.ScheduleTellOnce(TimeSpan.FromSeconds(seconds), mailbox.Self, message)

        let rec loop () =
            actor {
                let! message = mailbox.Receive()

                match message with
                | :? (string * string * string * string * string) as request ->
                    let (messageType, p1, p2, _, _) = request

                    match messageType with
                    | "StartClient" ->
                        printerRef <! sprintf "Client %s started: %d users, %d clients, server %s:%d" clientID usersCount clientsCount serverIP serverPort
                        mailbox.Self <! ("RegisterClient", "", "", "", "")
                    | "RegisterClient" -> // p1 = "retry" after the first attempt
                        // Ask the server to register this client, and ask again every 3 s until it answers
                        if isNull server then
                            if p1 = "retry" then
                                printerRef <! sprintf "No answer from the server at %s:%d yet, asking again" serverIP serverPort

                            serverSelection <! ("ClientRegistration", clientID, clientIP, string clientPort)
                            scheduleSelf 3.0 ("RegisterClient", "retry", "", "", "")
                    | "ACKClientRegistration" -> // p1 = message
                        // On the first answer, start the offline/online cycle and register the first user
                        if isNull server then
                            server <- mailbox.Sender()
                            printerRef <! p1
                            scheduleSelf 10.0 ("ClientActorGoOffline", "", "", "", "")
                            mailbox.Self <! ("RegisterUser", "1", "", "", "")
                    | "RegisterUser" -> // p1 = rank of the user to register, from 1 to usersCount
                        let rank = toInt p1
                        let userNumber = userNumbersByRank.[rank - 1]
                        let userID = clientID + "_" + string userNumber
                        let userRef = spawn system ("User_" + userID) UserActor
                        userActorRefMap <- Map.add userID (userRef, userNumber, rank) userActorRefMap
                        server <! ("UserRegistration", clientID, userID, string ((usersCount - 1) / rank))

                        // Register one user every 0.05 s
                        if rank < usersCount then
                            scheduleSelf 0.05 ("RegisterUser", string (rank + 1), "", "", "")
                    | "ACKUserRegistration" -> // p1, p2 = userID, message
                        printerRef <! p2

                        match Map.tryFind p1 userActorRefMap with
                        | Some(userRef, userNumber, rank) ->
                            registeredUserIDs.Add p1
                            userRef <! StartUserActor(userNumber, TimeSpan.FromMilliseconds(float baseTweetInterval * float rank), server)
                        | None -> ()
                    | "ClientActorGoOffline" ->
                        // Take a random 10% of the registered users offline and bring the previous batch back online
                        let batchSize = registeredUserIDs.Count / 10
                        let mutable goingOfflineSet = Set.empty

                        while goingOfflineSet.Count < batchSize do
                            let candidate = registeredUserIDs.[Random.Shared.Next registeredUserIDs.Count]

                            if not (currentlyOfflineUsersSet.Contains candidate) then
                                goingOfflineSet <- Set.add candidate goingOfflineSet

                        for userID in goingOfflineSet do
                            server <! ("ServerGoOffline", clientID, userID, "")
                            let (userRef, _, _) = userActorRefMap.[userID]
                            userRef <! UserActorGoOffline

                        for userID in currentlyOfflineUsersSet do
                            server <! ("ServerGoOnline", clientID, userID, "")

                        currentlyOfflineUsersSet <- goingOfflineSet
                        scheduleSelf 10.0 ("ClientActorGoOffline", "", "", "", "")
                    | "ClientGoOnline" -> // p1 = userID; the server has shown the user's feed
                        match Map.tryFind p1 userActorRefMap with
                        | Some(userRef, _, _) -> userRef <! UserActorGoOnline
                        | None -> ()
                    | _ -> ()
                | _ -> mailbox.Unhandled message

                return! loop ()
            }

        loop ()

    let boss = spawn system "AdminActor" ClientActor
    boss <! ("StartClient", "", "", "", "")
    system.WhenTerminated.Wait()
