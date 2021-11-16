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
    subsstr: string;
    timeStamp: DateTime;
}

type AckUserReg = {
    messageName: string;
    userID: string;
    message: string;
}

type Ready = {
    messageName: string;
    userID: string;
    clientsList: list<string>;
    server: ActorSelection;
    users: int;
    clientID: string;
    hashtagsList: list<string>;
    time: int;
}

type Offline = {
    messageName: string;
}