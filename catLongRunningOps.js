--catalog SLOW mongoDB queries
catLongRunningOps = function(maxSecsRunning) {
    db.currentOp().inprog.forEach(
        function(d) {
            if (d.secs_running > maxSecsRunning && d.op == "query" && !d.ns.startsWith("local")) {
                print("---------------------------")   
                print("secs_running:", d.secs_running)
                print("opid:", d.opid)
                print("host:", d.client)
                print("collection:", d.ns)   
                printjson(d.query);
            }
        })
};
