// Hsiang-Yuan Liao  DOSP Project 2  Gosiip Algorithm

open System
open Akka.Actor
open Akka.FSharp


let system = ActorSystem.Create("Alex")
let nodeNamePrefix = "Node"
let globalStopWatch = System.Diagnostics.Stopwatch()

// parameters that can be changed
let maxRumorHeardTimes = 10
let periodicTimer = 50
let ratioDifference = 0.0000000001 //10^-10
let mutable consecutiveRounds = 3

let mutable nodeFaultRate:double = 0.0


type BossMessage = 
    | Start of int
    | EndPushSum of double[]
    | Enqueue of int
    | Dequeue of int
    | PushStart of int
    | QueueSender of bool
    | PushQueueSender of bool

type NodeMessage =
    | Rumor of int
    | Sending of int
    | Push of double[]
    | PushSender of bool

let getNodeActorRefByName name =
    select ("akka://" + system.Name + "/user/" + nodeNamePrefix + name) system

let getActorRefByName name =
    select ("akka://" + system.Name + "/user/"  + name) system

let buildFullNeighborArray (emptyArray:string array) nodeIdx =
    for i in 0 .. emptyArray.Length-1 do
            if (i+1) = nodeIdx then
                Array.set emptyArray i ""
            else
                Array.set emptyArray i ((i+1).ToString())

let buildLineNeighborArray (emptyArray:string array) nodeIdx =
    let leftIdx = nodeIdx-1
    let rightIdx = nodeIdx+1
    if leftIdx > 0 then
        Array.set emptyArray 0 (leftIdx.ToString())
    if rightIdx <= emptyArray.Length then
        Array.set emptyArray 1 (rightIdx.ToString())

let build2DNeighborArray (emptyArray:string array) nodeIdx (imperfect:bool)=

    let sideLen = sqrt(emptyArray.Length |> double) |> int
    (*
        suppose 2D Grid for 16 nodes
        1  2  3  4 
        5  6  7  8
        9 10 11 12
       13 14 15 16

    *)
    let rightIdx = nodeIdx + 1
    let leftIdx = nodeIdx - 1
    let topIdx = nodeIdx - sideLen
    let belowIdx = nodeIdx + sideLen

    if topIdx > 0 then
        Array.set emptyArray 0 (topIdx.ToString())
    if belowIdx <= emptyArray.Length then
        Array.set emptyArray 1 (belowIdx.ToString())
    if (nodeIdx % sideLen) <> 0 then
        Array.set emptyArray 2 (rightIdx.ToString())
    if (nodeIdx % sideLen) <> (1) then
        Array.set emptyArray 3 (leftIdx.ToString())

    // for imp2D to have an extra neighbor
    if imperfect then
        let rnd = Random()
        let mutable rndIdx = rnd.Next(emptyArray.Length)+1
        // make sure the random pick won't repeat
        while Array.Exists(emptyArray, fun element -> element = rndIdx.ToString() ) do
            rndIdx <- rnd.Next(emptyArray.Length)+1
        Array.set emptyArray 4 (rndIdx.ToString())
    


let boss = 
    spawn system "Boss"
        <| fun bossMailbox ->
            let mutable totalNumNodes = 0
            let mutable propogationCount = 0
            let nodeSet = Set.empty  
            let stopWatch = System.Diagnostics.Stopwatch()

            // implementing node fault rate
            let mutable orgNodeSet = Set.empty
            let mutable faultNodeSet = Set.empty
            let mutable faultNodeCount = 0

            let rec bossLoop (nodeSet:Set<int>) =
                actor {
                    let! (msg: BossMessage) = bossMailbox.Receive()
                    match msg with
                    | Start msg ->
                        totalNumNodes <- msg
                        for i in 1 .. totalNumNodes do
                            orgNodeSet <- orgNodeSet.Add(i)

                        stopWatch.Start()
                        let maxRcvTimes = maxRumorHeardTimes
                        getNodeActorRefByName "1" <! Rumor maxRcvTimes

                    | EndPushSum msg ->   
                        propogationCount <- propogationCount + 1
                        
                        printf "."

                        // end of push-sum algo if all nodes has it's own converged ratio
                        if propogationCount = totalNumNodes then
                            printfn ""
                            stopWatch.Stop()
                            //printfn "[BOSS] Time duration: %u,  s: %.1f,  w: %.1f,  Ratio:%.10f" stopWatch.ElapsedMilliseconds msg.[1] msg.[2] msg.[0]
                            
                            if nodeFaultRate <> 0.0 then
                                printfn "[BOSS] Number of Node dies: %d" faultNodeCount
                                printfn "[BOSS] Node fault rate: %s%%" (nodeFaultRate |> string )
                            printfn "[BOSS] Time duration: %u, Ratio:%.10f" stopWatch.ElapsedMilliseconds msg.[0]
                            Environment.Exit 1

                    | Enqueue msg ->
                        let nodeSet = nodeSet.Add(msg)
                        propogationCount <- propogationCount + 1
                        printf "."

                        // end of gossip algo if all nodes has at least heard of the rumor once
                        if propogationCount = totalNumNodes then
                            printfn ""
                            stopWatch.Stop()
                            if nodeFaultRate <> 0.0 then
                                printfn "[BOSS] Number of Node dies: %d" faultNodeCount
                                printfn "[BOSS] Node fault rate: %s%%" (nodeFaultRate |> string )
                            printfn "[BOSS] Time duration: %u" stopWatch.ElapsedMilliseconds
                            Environment.Exit 1
                            
                        return! bossLoop nodeSet
                    | Dequeue msg ->
                        let nodeSet = nodeSet.Remove(msg)
                        return! bossLoop nodeSet

                    | QueueSender msg ->

                        // As time passed
                        // if faultrate = 0.1, means 0.1%, means 0.001 probability
                        // maintain a faultNodeSet here, don't trigger the node to send rumor in the faultNodeSet
                        let rnd = Random()
                        if nodeFaultRate <> 0.0 then
                            for nodeIdx in 1 .. totalNumNodes do
                                // to see each node is going to die or not
                                if orgNodeSet.Contains(nodeIdx) then
                                    let dieNow = rnd.NextDouble() < ((nodeFaultRate / 100.0) / (periodicTimer|>double))
                                    if dieNow then
                                        faultNodeSet <- faultNodeSet.Add(nodeIdx)
                                        orgNodeSet <- orgNodeSet.Remove(nodeIdx)
                                        faultNodeCount <- faultNodeCount + 1
                                        //printfn "Node%d died" nodeIdx

                        if orgNodeSet.IsEmpty || nodeSet.IsEmpty then
                            printfn ""
                            stopWatch.Stop()
                            if nodeFaultRate <> 0.0 then
                                printfn "[BOSS] Number of Node dies: %d" faultNodeCount
                                printfn "[BOSS] Node fault rate: %s%%" (nodeFaultRate |> string )
                            printfn "[BOSS] Gossip failed, time duration: %u" stopWatch.ElapsedMilliseconds
                            Environment.Exit 1
        
                        if msg then
                            if not nodeSet.IsEmpty then
                                for nodeIdx in nodeSet do
                                    // let the node gossip if it is not died
                                    if not (faultNodeSet.Contains(nodeIdx)) then
                                        getNodeActorRefByName (nodeIdx.ToString()) <! Sending maxRumorHeardTimes

                    | PushStart msg ->                        
                        totalNumNodes <- msg
                        for i in 1 .. totalNumNodes do
                            orgNodeSet <- orgNodeSet.Add(i)
                        stopWatch.Start()
                        
                        for i in 1 .. totalNumNodes do
                            let nodename = i.ToString()
                            getNodeActorRefByName nodename <! Push [|i|>double; 1.0|]

                        return! bossLoop nodeSet
                    | PushQueueSender msg ->

                        // As time passed
                        // if faultrate = 0.1, means 0.1%, means 0.001 probability
                        // maintain a faultNodeSet here, don't trigger the node to send rumor in the faultNodeSet
                        let rnd = Random()
                        if nodeFaultRate <> 0.0 then
                            for nodeIdx in 1 .. totalNumNodes do
                                // to see each node is going to die or not
                                if orgNodeSet.Contains(nodeIdx) then
                                    let dieNow = rnd.NextDouble() < (nodeFaultRate / 100.0)
                                    if dieNow then
                                        faultNodeSet <- faultNodeSet.Add(nodeIdx)
                                        orgNodeSet <- orgNodeSet.Remove(nodeIdx)
                                        faultNodeCount <- faultNodeCount + 1
                                        //printfn "Node%d died" nodeIdx

                        if orgNodeSet.IsEmpty then
                            printfn ""
                            stopWatch.Stop()
                            if nodeFaultRate <> 0.0 then
                                printfn "[BOSS] Number of Node dies: %d" faultNodeCount
                                printfn "[BOSS] Node fault rate: %s%%" (nodeFaultRate |> string )
                            printfn "[BOSS] Push-sum failed, time duration: %u" stopWatch.ElapsedMilliseconds
                            Environment.Exit 1

                        for nodeIdx in 1 .. totalNumNodes do
                            let nodename = nodeIdx.ToString()
                            // let the node gossip if it is not died
                            if not (faultNodeSet.Contains(nodeIdx)) then
                                getNodeActorRefByName nodename <! PushSender true
                        return! bossLoop nodeSet

                    return! bossLoop nodeSet
                }
            bossLoop nodeSet





let nodes initialCounter topology numNodes (nborSet:Set<string>) (nodeMailbox:Actor<NodeMessage>) =
    let nodeName = nodeMailbox.Self.Path.Name
    let nodeIdx = nodeName.Substring(4) |> int
    
    // variables for push-sum
    let mutable (s:double) = 0.0
    let mutable (w:double) = 0.0
    let mutable lastRatio:double = 10000.0
    let mutable terminationCounter = 0
    
    

    let rec loop counter (nborSet:Set<string>) = actor {
        let! (msg: NodeMessage) = nodeMailbox.Receive()
        match msg with
        | Rumor msg->
            let maxCount = msg
            if counter < maxCount && counter >= 0 then
                if counter = 0 then
                    // register as "active" node
                    getActorRefByName "Boss" <! Enqueue nodeIdx
                let newCounter = counter + 1
                return! loop newCounter nborSet
            else if counter = maxCount then
                // unregister, not active now             
                getActorRefByName "Boss" <! Dequeue nodeIdx
                return! loop -1 nborSet
            else if counter < 0 then
                return! loop counter nborSet
            else
                printfn "\n[%s] Something wrong, counter: %d" nodeName counter

        // send a rumor to a random neighbor
        | Sending msg ->
            
            let maxCount = msg
            let rnd = Random()
            let tmp = (nborSet |> Array.ofSeq)
            let name = tmp.[rnd.Next(tmp.Length)]
            if counter > 0 && counter < maxCount then
                getNodeActorRefByName name <! Rumor maxCount
                
            return! loop counter nborSet 

        // keep adding received s and w before sending new push next round
        | Push msg ->
            if terminationCounter = consecutiveRounds then
                return! loop counter nborSet
            let rcvS = msg.[0]
            let rcvW = msg.[1]
            s <- s + rcvS
            w <- w + rcvW
            return! loop counter nborSet

            
        | PushSender msg ->
            if terminationCounter = consecutiveRounds then
                return! loop counter nborSet

            let ratio = s / w

            // check if ratio is converged, consecutiveRounds here is really important to accuracy and time cost
            //printfn "[%s] current ratio: %.10f" nodeName ratio
            if (abs (ratio-lastRatio)) <= ratioDifference then
                terminationCounter <- terminationCounter + 1
                if terminationCounter = consecutiveRounds then
                    getActorRefByName "Boss" <! EndPushSum [|ratio; s; w|]
                    return! loop counter nborSet
            else
                terminationCounter <- 0
            lastRatio <- ratio

            // send 0.5s and 0.5w to a random neighbor
            let rnd = Random()
            let tmp = (nborSet |> Array.ofSeq)
            let name = tmp.[rnd.Next(tmp.Length)]
            getNodeActorRefByName name <! Push [|(0.5*s); (0.5*w)|]

            // send 0.5s and 0.5w to node itself also
            nodeMailbox.Self.Tell (Push [|0.5*s; 0.5*w;|])

            // reset s and w           
            s <- 0.0
            w <- 0.0
            return! loop counter nborSet
            
         
        return! loop counter nborSet
    }
    loop initialCounter nborSet

// used to spawn nodes and build topology
let nodeSpawn numNodes topology = 
    // round up numNodes to perfect square,  if numNodes = 12, then let new numNodes = 4^2 = 16
    let square x = x * x
    let mutable newNumNodes = numNodes
    if topology = "2D" || topology = "imp2D" then
        newNumNodes <- square ((ceil(sqrt(numNodes |> double))) |> int)

    // build neighbor sets for each node according to the topology
    for i in 1 .. newNumNodes do
        let nodename = nodeNamePrefix + i.ToString()
        let tmpArray = Array.create newNumNodes ""    
        match topology with
            | "full" ->
                buildFullNeighborArray tmpArray i 
            | "2D" ->
                let imperfect = false
                build2DNeighborArray tmpArray i imperfect
            | "imp2D" ->
                let imperfect = true
                build2DNeighborArray tmpArray i imperfect
            | "line" ->
                buildLineNeighborArray tmpArray i
            | _ ->
                printfn "\n[Main] Wrong topology input!"
                Environment.Exit 1
        // clean up the neighbor array and convert it to sets
        let nborSet = (Array.filter ((<>) "") tmpArray) |> Set.ofArray
        spawn system nodename (nodes 0 topology newNumNodes nborSet) |> ignore
    newNumNodes

[<EntryPoint>]
let main argv =
    try
        globalStopWatch.Start()
        let mutable numNodes = argv.[0] |> int
        let topology = argv.[1]
        let algorithm = argv.[2]
        

 
            
        
        let inbox = Inbox.Create(system)
        let mutable lastTStamp = globalStopWatch.ElapsedMilliseconds
 
        
        //printfn "[Main] Building topology: %s ..." topology
        numNodes <- nodeSpawn numNodes topology
        //printfn "[Main] Topology build done, timestamp: %i" globalStopWatch.ElapsedMilliseconds
        

        //printfn "[Main] Start execution..."
        match algorithm with
            | "gossip" -> 
                // each node has a possiblility that dies during the algorithm
                if argv.Length = 4 then
                    nodeFaultRate <- argv.[3] |> double
                    


                (boss <! Start numNodes) 

                // let "active" nodes periodically send rumor to others
                lastTStamp <- globalStopWatch.ElapsedMilliseconds
                while true do
                    if (globalStopWatch.ElapsedMilliseconds) - lastTStamp  >= (periodicTimer |> int64) then 
                        lastTStamp <- globalStopWatch.ElapsedMilliseconds
                        inbox.Send(boss, QueueSender true)

            | "push-sum" -> 
                (boss <! PushStart numNodes)
                // the optional 4th input is to set consecutive rounds, default is 3
                if argv.Length = 4 then
                    consecutiveRounds <- argv.[3] |> int
                    printfn "consecutiveRound: %d" consecutiveRounds
                
                // each node has a possiblility that dies during the algorithm
                if argv.Length = 5 then
                    nodeFaultRate <- argv.[4] |> double
                    

                // let nodes periodically send push to itself and others
                lastTStamp <- globalStopWatch.ElapsedMilliseconds
                while true do
                    if (globalStopWatch.ElapsedMilliseconds) - lastTStamp  >= (periodicTimer |> int64) then 
                        lastTStamp <- globalStopWatch.ElapsedMilliseconds
                        (boss <! PushQueueSender true)
            | _ ->
                printfn "\n[Main] Incorrect algorithm input!\n"
                Environment.Exit 1
    
    with | :? IndexOutOfRangeException ->
            printfn "\n[Main] Incorrect Inputs or IndexOutOfRangeException!\n"

         | :?  FormatException ->
            printfn "\n[Main] FormatException!\n"
            
    
    
    
    0 // return an integer exit code
