#r "nuget: Akka.FSharp"

open Akka.Actor
open Akka.FSharp
open System

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

(*type Ready = {
    messageName: string;
    userID: string;
    clientsList: list<string>;
    server: ActorSelection;
    usersCount: int;
    clientID: string;
    hashtagsList: list<string>;
    interval: int;
}

type StartTweet = {
    messageName: string;
}

type StartOtherAction = {
    messageName: string;
}*)

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