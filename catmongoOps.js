catMongoOps = function() {db.currentOp().inprog.forEach(
  function( d ) {
    if ( d.op == "query" && !d.ns.startsWith("local")  ) {
        print("---------------------------")    
        print("secs_running:", d.secs_running)
        print("opid:", d.opid)
        print("host:", d.client_s)
        print("collection:", d.ns)    
        printjson(d.query);
    }
})};
catMongoOps();
