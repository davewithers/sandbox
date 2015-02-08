-- kill longrunning Mongo Ops
killLongRunningOps = function(maxSecsRunning) {
    currOp = db.currentOp();
    for (oper in currOp.inprog) {
        op = currOp.inprog[oper-0];
        if (op.secs_running > maxSecsRunning && op.op == "query" && !op.ns.startsWith("local") && op.ns.match(/procon.*/) ) {
            print("Killing opId: " + op.opid
            + " running over for secs: "
            + op.secs_running);
            db.killOp(op.opid);
        }
    }
};

-- sample execution for safely killing all OPS > 200 secs:
-- killLongRunningOps(200)
