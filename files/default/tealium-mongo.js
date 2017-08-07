
/* 
 *
 * tealium-mongo.js -- Utility functions for Tealium Databases
 *
 * Usage:
 *  - Connect to a Mongos or Mongod
 *  - load("/<PATH>/tealium-mongo.js")
 *
 */

var Tongo = {

   slow_ops : function(secs) {

      var mongo = db.getMongo();
      var admin = mongo.getDB('admin');
      ops = admin.$cmd.sys.inprog.findOne( { "secs_running" : { "$gt" : secs } } );
      ops.inprog.forEach( function(sop) {
	 print("opid : " + sop["opid"]);
	 print("op : " + sop["op"]);
	 print("ns : " + sop["ns"]);
	 print("client : " + sop["client_s"]);
	 print("secs running : " + sop["secs_running"]);
	 print();
      });
   },

   datacloud : {

      // Returns average chunk size in KB for given collection and shard.
      avg_chunk_size : function(coll, shard) {

	 var collRE = RegExp( "^" + RegExp.escape("datacloud." + coll + "") + "-.*" );
	 var mongo = db.getMongo();
	 var config = mongo.getDB('config');
	 var dc = mongo.getDB('datacloud');

	 cnt = config.chunks.count({"_id":collRE, "shard":shard});

         var stats = dc.runCommand({ "collStats": coll, "scale" : 1024 });
	 var size = stats.shards[ shard ].size;

	 return Math.floor(size / cnt);
      },


      // Make the chunks more whittle.  (That was awful. Sorry.)
      // [Turn off the cluster balancer before using this.]
      whittle : function(coll, shard) {

	 var mongo = db.getMongo();
	 var config = mongo.getDB('config');
	 var admin = mongo.getDB('admin');
	 var dc = mongo.getDB('datacloud');
	 var collLongName = "datacloud." + coll;
	 var collRE = RegExp( "^" + RegExp.escape(collLongName) + "-.*" );
	 var howMany = 0;
	 var key;

         var stats = dc.runCommand({ "collStats": coll, "scale" : 1024 });

	 if ( !stats.sharded ) {
	    print( "Collection " + coll + " is not sharded." );
	    return;
	 }

	 var avgObjSize = stats.shards[ shard ].avgObjSize;

	 var collInfo = config.collections.findOne({"_id" : collLongName});
	 assert.eq(config.getLastError(), null);
	 assert.neq(collInfo, null);

	 for (k in collInfo.key) {
	    if (k != "shard_token") {
	       key = k;
	    }
	 }

	 avgChunkSize = this.avg_chunk_size(coll, shard);

	 print("Shard key = { shard_token : 1, " + key + " : 1 }");
	 print("Average chunk size = " + avgChunkSize + "KB.");
	 print("Average document size = " + avgObjSize + "KB.");

	 while (avgChunkSize > 65536) {

	    print("Looking for chunks > 64MB to split...");

	    var chunks = config.chunks.find({"_id":collRE, "shard":shard}).toArray()
	    
	    var chunk;
	    while (chunk = chunks.shift()) {
	       var countCmd = {};
	       countCmd["count"] = coll;
	       countCmd["query"] = {};
	       countCmd["query"]["shard_token"] = {};
	       countCmd["query"]["shard_token"]["$gte"] = chunk.min.shard_token;
	       countCmd["query"]["shard_token"]["$lte"] = chunk.max.shard_token;
	       countCmd["query"][key] = {};
	       countCmd["query"][key]["$gte"] = chunk.min[key];
	       countCmd["query"][key]["$lt"] = chunk.max[key];
	       var objCount = dc.runCommand( countCmd ).n;
	       assert.eq(dc.getLastError(), null);
	       var estChunkSize = objCount * avgObjSize;

	       //print("\n\nLooking at chunk " + chunk._id);
	       //print("Number of documents in chunk = " + objCount);
	       //print("Estimated chunk size = " + estChunkSize + "KB.");

	       if (estChunkSize > 65536) {
		  var splitQuery = {};
		  splitQuery["shard_token"] = chunk.min.shard_token;
		  splitQuery[key] = chunk.min[key];
		  //print("Splitting chunk " + splitCount + " times.");
		  admin.runCommand( {
		     split : collLongName,
		     find : splitQuery
		  } ); 
		  assert.eq(admin.getLastError(), null);
		  howMany++;

		  // var fritter = 10000000;
		  // while (fritter--) {}

	       }
	    }

	    print("Split " + howMany + " chunks in collection " + coll + " on shard " + shard + ".")

	    avgChunkSize = this.avg_chunk_size(coll, shard);
	    print("Average chunk size = " + avgChunkSize + "KB.");
	 }
      },


      // Find visitors last seen some months ago and store their IDs and visit IDs in datacloud_junk.expired_visitors.
      // Don't have an index to support this anymore.  Don't run it.
      stale : function(months) {

	 if (months < 1) {
	    print("Number of months must be at least 1.");
	    return;
	 }

	 var then = ISODate()
	 then.setMonth(then.getMonth() - months)
	 var deadtime = then.getTime();

	 print("cutoff date = " + then);
	 print("Mapping " + months + "-month old visitors into collection \'datacloud_junk.expired_visitors\'.");

	 var mongo = db.getMongo()
	 var junk = mongo.getDB('datacloud_junk')
	 var dc = mongo.getDB('datacloud')

	 var start_time = ISODate().getTime();
	 var vp_count = 0;
	 var v_count = 0;

	 var expired = dc.visitor_profiles.find( { "dates.last_visit_start_ts" : { "$lt" : deadtime }},
						 { "_id" : 1 }
					       ).addOption(DBQuery.Option.noTimeout);
	 expired.forEach( function(visitor) {
	    var ex_doc = { "_id" : visitor._id };
	    var linked_visits = [];
	    vp_count++;
	    visits = dc.visitor_visits.find( { "visitor_id" : visitor._id }, { "_id" : 1 } ).addOption(DBQuery.Option.noTimeout).toArray(); 
	    // visits.forEach( function(visit) {
	    //    v_count++;
	    //    linked_visits.push(visit._id);
	    // });

	    v_count += visits.length
	    ex_doc["visits"] = visits;

	    junk.expired_visitors.insert(ex_doc);
	    // assert.eq(junk.getLastError(), null)
	 });

	 var end_time = ISODate().getTime();
	 var elapsed = end_time - start_time;
	 print("Job completed in " + elapsed/1000 + " secs.");
	 print("Mapped " + vp_count + " visitor profiles and " + v_count + " visits.")
      },

      // Find one-visit visitors last seen some days ago and store their IDs and visit IDs in datacloud_junk.expired_visitors.
      // (Index to support this function has been removed!)
      onesies : function(days, batch_size) {

	 if (days < 1) {
	    print("Number of days must be at least 1.");
	    return;
	 }

	 var now = ISODate()
	 var then = new Date(now.getFullYear(), now.getMonth(), now.getDate() - days)
	 var deadtime = then.getTime();

	 print("Cutoff date = " + then);
	 print("Mapping " + days + "-day old one-visit visitors into collection \'datacloud_junk.expired_visitors\'.");

	 var mongo = db.getMongo()
	 var junk = mongo.getDB('datacloud_junk')
	 var dc = mongo.getDB('datacloud')

	 var start_time = ISODate().getTime();
	 var vp_count = 0;
	 var v_count = 0;

	 var expired = dc.visitor_profiles.find( { "shard_token" : { "$ne" : 0 },
						   "dates.last_visit_start_ts" : { "$lt" : deadtime },
						   "metrics.22" : 1 },
						 { "_id" : 1, "shard_token" : 1 }
					       ).hint(
						     "shard_token_1_dates.last_visit_start_ts_1_metrics.22_1_badges_1"
					       ).batchSize(batch_size).addOption(DBQuery.Option.noTimeout);

	 while (expired.hasNext()) {

	    var ex_docs = [];

	    while (expired.objsLeftInBatch()) {
					       
	       var visitor = expired.next();
	       var ex_doc = { "_id" : visitor._id };
	       var linked_visits = [];
	       vp_count++;
	       visits = dc.visitor_visits.find( { "shard_token" : visitor.shard_token, "visitor_id" : visitor._id },
						{ "_id" : 1 }
		                              ).addOption(DBQuery.Option.noTimeout); 
	       visits.forEach( function(visit) {
		  v_count++;
		  linked_visits.push(visit._id);
	       });
	       ex_doc["visits"] = linked_visits;
	       ex_docs.push(ex_doc);
	    }

	    junk.expired_visitors.insert(ex_docs);
	    //assert.eq(junk.getLastError(), null);
	 }

	 var end_time = ISODate().getTime();
	 var elapsed = end_time - start_time;
	 print("Job completed in " + elapsed/1000 + " secs.");
	 print("Mapped " + vp_count + " visitor profiles and " + v_count + " visits.")
      },

      orphans : function() {

	 assert.eq(db.serverStatus().process, "mongos")

	 print("Mapping orphaned visits into collection \'datacloud_junk.orphaned_visits\'.");

	 var mongos = db.getMongo()
	 var junk = mongos.getDB('datacloud_junk')
	 var dc = mongos.getDB('datacloud')

	 var start_time = ISODate().getTime();
	 var v_count = 0;

	 var visits = dc.visitor_visits.find( { }, { "_id" : 1, "visitor_id" : 1 }).addOption(DBQuery.Option.noTimeout);
	 visits.forEach( function(visit) {
	    var vp_count = dc.visitor_profiles.count( { "_id" : visit.visitor_id } );
	    if (vp_count == 0) {
	       v_count++;
	       junk.orphaned_visits.insert({ "_id" : visit._id });
	       assert.eq(junk.getLastError(), null)
	    }
	 });

	 var end_time = ISODate().getTime();
	 var elapsed = end_time - start_time;
	 print("Job completed in " + elapsed/1000 + " secs.");
	 print("Mapped " + v_count + " orphaned visits.")

      },

      clean_expired : function(batch_size) {

	 assert.eq(db.serverStatus().process, "mongos");

	 var mongos = db.getMongo();
	 var junk = mongos.getDB('datacloud_junk');
	 var dc = mongos.getDB('datacloud');

	 var profiles_removed = 0;
	 var visits_removed = 0;

	 var start_time = ISODate().getTime();

	 var expired = junk.expired_visitors.find().readPref('nearest').batchSize(batch_size).addOption(DBQuery.Option.noTimeout);
	 
	 while (expired.hasNext()) {

	    var profiles = [];
	    var visits = [];

	    while (expired.objsLeftInBatch()) {

	       var ex = expired.next();
	       profiles = profiles.concat(ex._id);
	       visits = visits.concat(ex.visits);

	    }

	    junk.expired_visitors.remove( { "_id" : { "$in" : profiles } } );

	    dc.visitor_profiles.remove( { "_id" : { "$in" : profiles } } );
	    profiles_removed += dc.getLastErrorObj().n;

	    dc.visitor_visits.remove( { "_id" : { "$in" : visits } } );
	    visits_removed += dc.getLastErrorObj().n;

	 }

	 var end_time = ISODate().getTime();
	 var elapsed = end_time - start_time;
	 print("Job completed in " + elapsed/1000 + " secs.");
	 print("Removed " + profiles_removed + " visitor profiles.");
	 print("Removed " + visits_removed + " visits.");
      },

      clean_orphans : function(batch_size) {

	 assert.eq(db.serverStatus().process, "mongos")

	 var mongos = db.getMongo();
	 var junk = mongos.getDB('datacloud_junk');
	 var dc = mongos.getDB('datacloud');

	 var orphans_removed = 0;
	 var start_time = ISODate().getTime();

	 var orphans = junk.orphaned_visits.find().readPref('nearest').batchSize(batch_size).addOption(DBQuery.Option.noTimeout);
	 while (orphans.hasNext()) {

	    var visits = [];

	    while (orphans.objsLeftInBatch()) {
	       var orphan = orphans.next();
	       visits.concat(orphan._id);
	    }

	    dc.visitor_visits.remove( { "_id" : { "$in" : visits } } );
	    orphans_removed += dc.getLastErrorObj().n;

	    junk.orphaned_visits.remove( { "_id" : { "$in" : visits } } );

	 }

	 var end_time = ISODate().getTime();
	 var elapsed = end_time - start_time;
	 print("Job completed in " + elapsed/1000 + " secs.");
	 print("Removed " + orphans_removed + " orphaned visits.");
      },

      plan_b : function(range_start, range_end) {

	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');
	 var done = 0;
	 var targets = [];

	 while (!done) {
	    var now = ISODate()
	    targets = dc.visitor_profiles.find( { "shard_token": { "$gte" : range_start, "$lte" : range_end },
						  "expire_at": { "$lt" : now } },
						   { "_id" : 1, "shard_token" : 1 }
					      ).limit(100).batchSize(100).toArray();
	    assert.eq(dc.getLastError(), null);
	    if (!targets.length) {
	       done = 1;
	    } else {
	       while (profile = targets.shift()) {
		  dc.visitor_visits.remove({"shard_token" : profile.shard_token, "visitor_id" : profile._id});
		  dc.visitor_profiles.remove({"shard_token" : profile.shard_token, "_id" : profile._id});
	       }
	    }
	 }
	 print("Purge (" + range_start + ", " + range_end + ") complete.")
      },

      plan_x : function(range_start, range_end) {

	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');
	 var done = 0;

	 while (!done) {
	    var now = ISODate()
	    var targets = dc.visitor_profiles.find( { "shard_token": { "$gte" : range_start, "$lte" : range_end },
						      "expire_at": { "$exists" : 0 } },
						    { "_id" : 1, "shard_token" : 1 }
						  ).addOption(DBQuery.Option.noTimeout);
	    if (targets.count() == 0) {
	       done = 1;
	    } else {
	       if (targets.hasNext()) {
		  targets.forEach( function(profile) {
		     dc.visitor_visits.remove({"shard_token" : profile.shard_token, "visitor_id" : profile._id});
		     dc.visitor_profiles.remove({"shard_token" : profile.shard_token, "_id" : profile._id});
		  });
	       }
	    }
	 }
	 print("Purge (" + range_start + ", " + range_end + ") complete.")
      },

      vqsq_drop : function() {
	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');

	 dc.getCollectionNames().forEach(function(c){
	    if (c.match("visitor_queryresult_streams_query.*")){
	       dc.runCommand({"drop": c});
	    }
	 });
      },

      purge_visitors_by_acct : function(acct) {

	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');
	 var done = 0;

	 while (!done) {
	    var targets = dc.visitor_profiles.find( { "properties.account": acct },
						    { "_id" : 1, "shard_token" : 1 }
						  ).hint("properties.account_1_properties.profile_1__secondary_ids_1").limit(1000).batchSize(1000).toArray();
	    if (!targets.length) {
	       done = 1;
	    } else {
	       while (profile = targets.shift()) {
		  dc.visitor_visits.remove({"shard_token" : profile.shard_token, "visitor_id" : profile._id});
		  dc.visitor_profiles.remove({"shard_token" : profile.shard_token, "_id" : profile._id});
	       }
	    }
	 }
	 print("Purge complete.")
      },

      purge_visitors_by_id : function(id) {

	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');
	 var done = 0;

	 dc.visitor_profiles.remove({"_id" : id});
	 dc.visitor_visits.remove({"visitor_id" : id});
	 print("Purge complete.")
      },

      purge_visitors_by_sec_id : function(acct,profile,sid) {

	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');
	 var done = 0;

	 while (!done) {
	    var targets = dc.visitor_profiles.find( { "properties.account": acct,
						      "properties.profile": profile,
						      "_secondary_ids": sid },
						    { "_id" : 1, "shard_token" : 1 }
						  ).hint("properties.account_1_properties.profile_1__secondary_ids_1").limit(500).batchSize(500).toArray();
	    if (!targets.length) {
	       done = 1;
	    } else {
	       while (profile = targets.shift()) {
		  dc.visitor_visits.remove({"shard_token" : profile.shard_token, "visitor_id" : profile._id});
		  dc.visitor_profiles.remove({"shard_token" : profile.shard_token, "_id" : profile._id});
	       }
	    }
	 }
	 print("Purge complete.")
      },

      debenhams_cleanup : function() {

	 var mongo = db.getMongo();
	 var dc = mongo.getDB('datacloud');
	 var done = 0;

	 while (!done) {
	    var targets = dc.visitor_profiles.find( { "properties.account": "debenhams",
						      "properties.profile": "main",
						      "secondary_ids.5215" : {$in : [ "-1002", "$customer_id" ] },
						      "properties.5213" : {$in : [ "-1002", "$customer_id" ] }},
						    { "_id" : 1, "shard_token" : 1 }
						  ).hint("properties.account_1_properties.profile_1__secondary_ids_1").limit(1000).batchSize(1000).toArray();
	    if (!targets.length) {
	       done = 1;
	    } else {
	       while (profile = targets.shift()) {
		  dc.visitor_visits.remove({"shard_token" : profile.shard_token, "visitor_id" : profile._id});
		  dc.visitor_profiles.remove({"shard_token" : profile.shard_token, "_id" : profile._id});
	       }
	    }
	 }
	 print("Purge complete.")
      },

      add_region_tag_ranges : function (ns) {
	 assert.eq(db.serverStatus().process, "mongos");
	 var mongos = db.getMongo();
	 var config = mongos.getDB('config');

	 ranges = [
	     { "min":{"shard_token": MinKey}, "max":{"shard_token":11000000}, "tag":"us-west-1"},
	     { "min":{"shard_token":11000000}, "max":{"shard_token":11999999}, "tag":"us-west-1"},
	     { "min":{"shard_token":12000000}, "max":{"shard_token":12999999}, "tag":"us-west-2"},
	     { "min":{"shard_token":21000000}, "max":{"shard_token":21999999}, "tag":"us-east-1"},
	     { "min":{"shard_token":31000000}, "max":{"shard_token":31999999}, "tag":"eu-west-1"},
	     { "min":{"shard_token":41000000}, "max":{"shard_token":41999999}, "tag":"sa-east-1"},
	     { "min":{"shard_token":51000000}, "max":{"shard_token":51999999}, "tag":"ap-northeast-1"},
	     { "min":{"shard_token":61000000}, "max":{"shard_token":61999999}, "tag":"ap-southeast-1"},
	     { "min":{"shard_token":62000000}, "max":{"shard_token":62999999}, "tag":"ap-southeast-2"},
	     { "min":{"shard_token":71000000}, "max":{"shard_token":71999999}, "tag":"eu-central-1"}
	 ]

	 ranges.forEach( function(range) {

	    config.tags.update( { "_id" : { "ns" : ns , "min" : range.min }} ,
				{ _id: { "ns" : ns , "min" : range.min },
				  "ns" : ns,
				  "min" : range.min,
				  "max" : range.max,
				  "tag" : range.tag
				},
				true );
	    assert.eq(config.getLastError(), null);
	 });
      },

      setup_visit_collections : function (year) {

	 assert.eq(db.serverStatus().process, "mongos");

	 var mongos = db.getMongo();
	 var admin = mongos.getDB('admin');

	 prefix = "datacloud.visitor_visits_" + year.toString() + "_";

	 for(i=1; i<=12; i=i+1) {
	    ns = prefix + i.zeroPad(2);
	    admin.runCommand({ "shardCollection" : ns, "key" : { "shard_token" : 1, "_id" : 1 } })
	    this.add_region_tag_ranges(ns);
	 }
      },

      visitor_visit_stats : function (stat) {

	 if (stat == '') {
	    print("Usage: visitor_visit_stats(<stat>)");
	    print("Where stat is a collection statistic, e.g. 'count'.");
	    return;
	 }

	 var mongos = db.getMongo();
	 var config = mongos.getDB('config');
	 var dc = mongos.getDB('datacloud');

	 colls = config.collections.find({_id:/datacloud.visitor_visits_/},{_id:1}).toArray();

	 colls.forEach( function(c) {
	    shortName = c._id.match("datacloud\.(.*)")[1]
	    stats = dc.runCommand({ "collStats": shortName, "scale" : 1024 });
	    print(c._id + ":     " + stats[stat]);
	 });
      },

      create_indexes : function() {
	 var mongos = db.getMongo();
	 var dc = mongos.getDB('datacloud');

	 dc.runCommand({ "createIndexes" : "visitor_profiles", indexes : [
	    { "key" : { "properties.account" : 1, "properties.profile" : 1, "_secondary_ids" : 1 },
	      "name" : "properties.account_1_properties.profile_1__secondary_ids_1", "background" : true },
	    { "key" : { "shard_token" : 1, "expire_at" : 1 }, "name" : "shard_token_1_expire_at_1", "background" : true },
	    { "key" : { "shard_token" : 1, "properties.account" : 1, "properties.profile" : 1, "dates.last_visit_start_ts" : 1 },
	      "name" : "shard_token_1_properties.account_1_properties.profile_1_dates.last_visit_start_ts_1",
	      "background" : true }
	 ]});
   
	 dc.runCommand({ "createIndexes" : "active_queries", indexes : [
	    { "key" : { "query.id" : 1 }, "name" : "query.id_1", "background" : true },
	    { "key" : { "last_inquiry" : 1 }, "name" : "last_inquiry_1", "expireAfterSeconds" : 180, "background" : true },
	    { "key" : { "updated_on" : 1, "published" : 1 }, "name" : "updated_on_1_published_1", "background" : true }
	 ]});

	 dc.runCommand({ "createIndexes" : "bulk_download_keys", indexes : [
	    { "key" : { "account" : 1, "profile" : 1 }, "name" : "account_1_profile_1", "background" : true } 
	 ]});

	 dc.runCommand({ "createIndexes" : "bulk_downloaded_files", indexes : [
	    { "key" : { "file.name" : 1, "account" : 1, "profile" : 1 },
	      "name" : "file.name_1_account_1_profile_1",
	      "background" : true }
	]});

	 dc.runCommand({ "createIndexes" : "data_access_clusters", indexes : [
	    { "key" : { "account" : 1, "profile" : 1, "region" : 1 },
	      "name" : "account_1_profile_1_region_1",
	      "background" : true }
	 ]});

	 dc.runCommand({ "createIndexes" : "datacloud_profile_publish_queue", indexes : [
	    { "key" : { "publish_time" : 1 },
	       "name" : "publish_time_1",
	       "expireAfterSeconds" : NumberLong(180),
	       "background" : true }
	 ]});

	 dc.runCommand({ "createIndexes" : "delayed_actions", indexes : [
	    { "key" : { "shard_token" : 1, "visitor_id" : 1, "audience_query_id" : 1,
			"service_definition_id" : 1, "action_id" : 1 }, 
	      "name" : "shard_token_1_visitor_id_1_audience_query_id_1_service_definition_id_1_action_id_1",
	      "background" : true },
	    { "key" : { "_id" : 1, "shard_token" : 1 }, "name" : "_id_1_shard_token_1", "background" : true },
	    { "key" : { "shard_token" : 1, "execute_at" : 1 }, "name" : "shard_token_1_execute_at_1", "background" : true }
	 ]});

	 dc.runCommand({ "createIndexes" : "visitor_queryresult_agg", indexes : [
	    { "key" : { "last_update" : 1 }, "name" : "last_update_1", "expireAfterSeconds" : 600, "background" : true } 
	 ]});
      }

   },

   datacloud_audit : {

      create_indexes : function() {

	 var mongos = db.getMongo();
	 var da = mongos.getDB('datacloud_audit');

	 da.runCommand({ "createIndexes" : "datacloud_activity_amazon_uploader", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1",
              "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_activity_bulk_downloader", "indexes" : [
	    { "key" : { "_id" : 1 }, "name" : "_id_", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1 }, "name" : "account_1_profile_1_start_time_1",
	      "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
	      "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_activity_data", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1 }, 
              "name" : "account_1_profile_1_start_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_activity_eventstream_processor", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_activity_vendor_action", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "action_id" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_action_id_1_start_time_1_end_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_audit_amazon_uploader", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1 },
              "name" : "account_1_profile_1_start_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_audit_archived_filtered_stream", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1 },
              "name" : "account_1_profile_1_start_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_audit_event_processor", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1 },
              "name" : "account_1_profile_1_start_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "date_ts" : 1 },
              "name" : "account_1_profile_1_date_ts_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1, "date_ts" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1_date_ts_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "datacloud_audit_visitor_service", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1 },
              "name" : "account_1_profile_1_start_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "date_ts" : 1 },
              "name" : "account_1_profile_1_date_ts_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1, "date_ts" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1_date_ts_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_start_time_1_end_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "eventstream_processor_data", "indexes" : [
	    { "key" : { "query_id" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "query_id_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "start_time" : 1 }, "name" : "start_time_1", "expireAfterSeconds" : 600,
              "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "eventstream_processor_metrics", "indexes" : [
	    { "key" : { "query_id" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "query_id_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "start_time" : 1 }, "name" : "start_time_1", "expireAfterSeconds" : 60,
              "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "trace_ids", "indexes" : [
	    { "key" : { "created" : 1 }, "name" : "created_1", "expireAfterSeconds" : 43200,
              "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "trace_steps", "indexes" : [
	    { "key" : { "event_time" : 1 }, "name" : "event_time_1", "expireAfterSeconds" : 43200,
              "background" : true },
	    { "key" : { "trace_id" : 1, "step_time" : 1 }, "name" : "trace_id_1_step_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "trace_transformations", "indexes" : [
	    { "key" : { "trace_id" : 1, "event_id" : 1 }, "name" : "trace_id_1_event_id_1", "background" : true },
	    { "key" : { "transformation_time" : 1 }, "name" : "transformation_time_1", "expireAfterSeconds" : 43200,
              "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "trace_triggers", "indexes" : [
	    { "key" : { "trigger_time" : 1 }, "name" : "trigger_time_1", "expireAfterSeconds" : 43200, "background" : true },
	    { "key" : { "trace_id" : 1, "event_id" : 1 }, "name" : "trace_id_1_event_id_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "vendor_action_audit_data", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "vendor" : 1, "action" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_vendor_1_action_1_start_time_1_end_time_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "vendor_id" : 1, "action_id" : 1, "start_time" : 1, "end_time" : 1 },
              "name" : "account_1_profile_1_vendor_id_1_action_id_1_start_time_1_end_time_1", "background" : true }
         ]});

	 da.runCommand({ "createIndexes" : "vendor_action_error_sample_data", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "vendor_id" : 1, "action_id" : 1, "time" : 1 },
              "name" : "account_1_profile_1_vendor_id_1_action_id_1_time_1", "background" : true }
         ]});

      }

   },

   core : {

      // Some of the legacy code (maybe even some new code) creates unneeded or just plain bogus indices.
      // Unfortunately some of those indices are probably enshrined here as well.  Boo.
      create_indexes : function() {

	 var mongos = db.getMongo();
	 var core = mongos.getDB('core');

	 core.runCommand({ "createIndexes" : "account_ip_whitelists", "indexes" : [
	    { "key" : { "account" : 1 }, "name" : "account_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "accounts", "indexes" : [
	    { "key" : { "account" : 1 }, "name" : "account_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "datacloud_profiles", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "version_info" : 1, "settings" : 1 },
	      "name" : "account_1_profile_1_version_info_1_settings_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1 }, "name" : "account_1_profile_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "free_trial_verifications", "indexes" : [
	    { "key" : { "created_at" : 1 }, "name" : "created_at_1",
	      "expireAfterSeconds" : NumberLong(3600), "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "login_events", "indexes" : [
	    { "key" : { "last_update" : 1 }, "name" : "last_update_1",
	      "expireAfterSeconds" : NumberLong(600), "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "permission_cache", "indexes" : [
	    { "key" : { "email" : 1, "account" : 1 }, "name" : "email_1_account_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "ping_sessions", "indexes" : [
	    { "key" : { "session_context_id" : 1 }, "name" : "session_context_id_1", "background" : true },
	    { "key" : { "ping_ts" : 1 }, "name" : "ping_ts_1", "expireAfterSeconds" : 15, "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "ping_sessions_test", "indexes" : [
	    { "key" : { "session_context_id" : 1 }, "name" : "session_context_id_1", "background" : true },
	    { "key" : { "ping_ts" : 1 }, "name" : "ping_ts_1", "expireAfterSeconds" : 15, "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "profile_versions", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "version" : 1 },
	      "name" : "account_1_profile_1_version_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "profiles", "indexes" : [
	    { "key" : { "account" : 1, "name" : 1 }, "name" : "account_1_name_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "publish_approvals", "indexes" : [
	    { "key" : { "account" : 1 }, "name" : "account_1", "background" : true },
	    { "key" : { "profile" : 1 }, "name" : "profile_1", "background" : true },
	    { "key" : { "userid" : 1 }, "name" : "userid_1", "background" : true },
	    { "key" : { "timestamp" : 1 }, "name" : "timestamp_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "sessions", "indexes" : [
	    { "key" : { "last_accessed" : 1 }, "name" : "last_accessed_1",
	      "expireAfterSeconds" : 25200, "background" : true },
	    { "key" : { "last_updated" : 1 }, "name" : "last_updated_1",
	      "expireAfterSeconds" : NumberLong(3600), "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "subscription_overages", "indexes" : [
	    { "key" : { "period_ended_at" : 1 }, "name" : "period_ended_at_1", "background" : true },
	    { "key" : { "account_code" : 1, "plan_code" : 1, "period_started_at" : 1, "period_ended_at" : 1 },
	      "name" : "account_code_1_plan_code_1_period_started_at_1_period_ended_at_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "tag_fingerprints", "indexes" : [
	    { "key" : { "_id" : NumberLong(1), "tag_id" : NumberLong(1) }, "name" : "_id_1_tag_id_1", "background" : true }
	 ]});

	 // This is insane.
	 core.runCommand({ "createIndexes" : "tags", "indexes" : [
	    { "key" : { "account" : 1 }, "name" : "account_1", "background" : true },
	    { "key" : { "profile" : 1 }, "name" : "profile_1", "background" : true },
	    { "key" : { "version" : 1 }, "name" : "version_1", "background" : true },
	    { "key" : { "url" : 1 }, "name" : "url_1", "background" : true },
	    { "key" : { "env" : 1 }, "name" : "env_1", "background" : true },
	    { "key" : { "useragent" : 1 }, "name" : "useragent_1", "background" : true },
	    { "key" : { "tagid" : 1 }, "name" : "tagid_1", "background" : true },
	    { "key" : { "loadrule" : 1 }, "name" : "loadrule_1", "background" : true },
	    { "key" : { "key" : 1 }, "name" : "key_1", "background" : true },
	    { "key" : { "period.interval" : 1 }, "name" : "period.interval_1", "background" : true },
	    { "key" : { "period.start" : 1 }, "name" : "period.start_1", "background" : true },
	    { "key" : { "period.end" : 1 }, "name" : "period.end_1", "background" : true }
	 ]});
	    
	 core.runCommand({ "createIndexes" : "user_tag_ratings", "indexes" : [
	    { "key" : { "userid" : 1 }, "name" : "userid_1", "background" : true },
	    { "key" : { "tagid" : 1 }, "name" : "tagid_1", "background" : true },
	    { "key" : { "timestamp" : 1 }, "name" : "timestamp_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "users", "indexes" : [
	    { "key" : { "email" : 1 }, "name" : "email_1", "background" : true },
	    { "key" : { "permissions" : 1, "status" : 1 }, "name" : "permissions_1_status_1", "background" : true },
	    { "key" : { "_id" : 1, "permission_exclusions" : 1 }, "name" : "_id_1_permission_exclusions_1", "background" : true }
	 ]});

	 core.runCommand({ "createIndexes" : "users_test", "indexes" : [
	    { "key" : { "email" : 1 }, "name" : "email_1", "background" : true }
	 ]});

      }
   },

   reporting : {

      create_indexes : function() {

	 var mongos = db.getMongo();
	 var rep = mongos.getDB('reporting');

	 rep.runCommand({ "createIndexes" : "reporting_log_files", "indexes" : [
	    { "key" : { "cdn" : 1, "file" : 1, "status" : 1 }, "name" : "cdn_1_file_1_status_1", "background" : true }
	 ]});

	 rep.runCommand({ "createIndexes" : "tag_usage_account", "indexes" : [
	    { "key" : { "shard_token" : 1, "account" : 1, "start" : 1, "end" : 1 },
	      "name" : "shard_token_1_account_1_start_1_end_1", "background" : true }
	 ]});

	 rep.runCommand({ "createIndexes" : "tag_usage_account_profile", "indexes" : [
	    { "key" : { "shard_token" : 1, "account" : 1, "profile" : 1, "start" : 1, "end" : 1 },
	      "name" : "shard_token_1_account_1_profile_1_start_1_end_1", "background" : true }
	 ]});

	 rep.runCommand({ "createIndexes" : "tags", "indexes" : [
	    { "key" : { "key" : 1, "period.interval" : 1, "period.start" : 1, "period.end" : 1 },
	      "name" : "key_1_period.interval_1_period.start_1_period.end_1", "background" : true },
	    { "key" : { "account" : 1, "profile" : 1, "period.interval" : 1, "period.start" : 1, "period.end" : 1 },
	      "name" : "account_1_profile_1_period.interval_1_period.start_1_period.end_1", "background" : true }
	 ]});
      }
   },

   server2server : {

      create_indexes : function() {

	 var mongos = db.getMongo();
	 var s2s = mongos.getDB('server2server');

	 s2s.runCommand({ "createIndexes" : "profiles", "indexes" : [
	    { "key" : { "account_name" : 1, "profile_name" : 1, "env" : 1 },
	      "name" : "account_name_1_profile_name_1_env_1", "background" : true }
	 ]});
      }
   },

   site_audit : {

      create_indexes : function() {

	 var mongos = db.getMongo();
	 var sa = mongos.getDB('site_audit');

	 sa.runCommand({ "createIndexes" : "audits", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1 }, "name" : "account_1_profile_1", "background" : true }
	 ]});

	 sa.runCommand({ "createIndexes" : "reports", "indexes" : [
	    { "key" : { "audit_id" : 1 }, "name" : "audit_id_1", "background" : true }
	 ]});

	 sa.runCommand({ "createIndexes" : "sitemaps", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1 }, "name" : "account_1_profile_1", "background" : true }
	 ]});

	 sa.runCommand({ "createIndexes" : "urls", "indexes" : [
	    { "key" : { "sitemap_url_id" : 1 }, "name" : "sitemap_url_id_1", "background" : true },
	    { "key" : { "report_id" : 1, "url" : 1, "created" : -1 },
	      "name" : "report_id_1_url_1_created_-1", "background" : true },
	    { "key" : { "report_id" : 1 }, "ns" : "site_audit.urls", "name" : "report_id_1", "background" : true }
	 ]});
      }
   },

   tiq_verifier : {

      create_indexes : function() {

	 var mongos = db.getMongo();
	 var tv = mongos.getDB('tiq_verifier');

	 tv.runCommand({ "createIndexes" : "verifier_data_spec", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "revision" : 1 },
	      "name" : "account_1_profile_1_revision_1", "background" : true }
	 ]});

	 tv.runCommand({ "createIndexes" : "verifier_test_config", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "revision" : 1 },
	      "name" : "account_1_profile_1_revision_1", "background" : true }
	 ]});

	 tv.runCommand({ "createIndexes" : "verifier_test_results", "indexes" : [
	    { "key" : { "account" : 1, "profile" : 1, "revision" : 1 },
	      "name" : "account_1_profile_1_revision_1", "background" : true }
	 ]});
      }
   }
}

print("Tongo.slow_ops(secs) - Report on DB operations that have been running for longer than given number of seconds.");
print("Tongo.datacloud.stale(months) - Create a collection of visitors last seen some months ago. DEPRECATED.");
print("Tongo.datacloud.onesies(days, batch_size) - Create a collection of one-visit visitors last seen some days ago.");
print("Tongo.datacloud.orphans() - Create a collection of visits associated with non-existent visitor profiles.");
print("Tongo.datacloud.clean_expired(batch_size) - Remove visitors and visits using the expired_visitors map.");
print("Tongo.datacloud.clean_orphans(batch_size) - Remove visits using the orphaned_visits map.");
print("Tongo.datacloud.plan_b(start, end) - Delete expired visitors and visits in given shard token range.");
print("Tongo.datacloud.plan_x(start, end) - Delete visitors and visits w/o expire_at and in given shard token range.");
print("Tongo.datacloud.vqsq_drop() - Drop visitor_queryresult_streams_query_* collections.");
print("Tongo.datacloud.purge_visitors_by_acct(<acct>) - Delete visitors and visits for a given account.");
print("Tongo.datacloud.purge_visitors_by_id(<id>) - Delete visitors and visits for a given visitor ID.");
print("Tongo.datacloud.purge_visitors_by_sec_id(<acct>,<profile>,<secondary id>) - Delete visitors and visits for a given account,profile, and secondary ID.");
print("Tongo.datacloud.avg_chunk_size(coll, shard) - Compute avg chunk size of given collection on given shard.");
print("Tongo.datacloud.whittle(coll, shard) - Split oversized chunks on given collection and shard.");
print("Tongo.datacloud.debenhams_cleanup() - Remove bogus Debenhams visitors based on a specifc (hardcoded) set of criteria.");
print("Tongo.datacloud.add_region_tag_ranges(ns) - Add Tealium std ranges to given ns.");
print("Tongo.datacloud.setup_visit_collections(year) - Shard visitor_visit_<year>_<month> for each month of given year.");
print("Tongo.datacloud.visitor_visit_stats(stat) - Output given stat for each datacloud.visitor_visits_* collection.");
print("Tongo.datacloud.create_indexes() - Create required indexes.");
print("Tongo.datacloud_audit.create_indexes() - Create required indexes.");
print("Tongo.core.create_indexes() - Create required indexes.");
print("Tongo.reporting.create_indexes() - Create required indexes.");
print("Tongo.server2server.create_indexes() - Create required indexes.");
print("Tongo.site_audit.create_indexes() - Create required indexes.");
print("Tongo.tiq_verifier.create_indexes() - Create required indexes.");

