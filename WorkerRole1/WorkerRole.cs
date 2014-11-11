using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;


namespace StatsWorker
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        private CloudQueue activityQueue = null;
        private readonly string prodDBString = "mongodb://rs1-1.mongo.blahgua.com:21191,rs1-2.mongo.blahgua.com:21191, rs1-3.mongo.blahgua.com:21191";
        private readonly string qaDBString = "mongodb://qa.db.blahgua.com:21191";
        private MongoClient mongoClient;
        private MongoServer mongoServer;
        private MongoDatabase statsDB;
        private MongoDatabase blahsDB;
        private MongoDatabase usersDB;
        private MongoDatabase infoDB;
        private bool isProd = false;

        // stat collections
        private MongoCollection<BlahStat> blahStats;
        private MongoCollection<UserStat> userStats;
        private MongoCollection<GroupStat> groupStats;
        private MongoCollection<SystemStat> systemStats;
        private MongoCollection<ActivityRecord> rawActivity;
        private MongoCollection<SimpleBlah> blahsCol;
        private MongoCollection<SimpleUser> usersCol;
        private MongoCollection<UserBlahStat> userBlahStats;
        private MongoCollection<WhatsNewInfo> whatsNewCol;
        private MongoCollection blahInfoCol;
        private MongoCollection userGroupInfoCol;

        public override void Run()
        {
            Trace.TraceInformation("HeardActivityQueue is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        private void InitializeActivityQueue()
        {
            try
            {
                if (CloudConfigurationManager.GetSetting("RunMode").Equals("prod", StringComparison.OrdinalIgnoreCase))
                    isProd = true;
                else
                    isProd = false;

                // Retrieve storage account from connection-string.
                string queueString;
                if (isProd)
                    queueString = CloudConfigurationManager.GetSetting("ProdConnectionString");
                else
                    queueString = CloudConfigurationManager.GetSetting("QAConnectionString");

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(queueString);

                // Create the queue client.
                CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

                // Retrieve a reference to a queue.
                activityQueue = queueClient.GetQueueReference("activityqueue");

                // Create the queue if it doesn't already exist.
                activityQueue.CreateIfNotExists();
            }
            catch (Exception e)
            {
                // Output the stack trace.
                Debug.WriteLine(e.Message);
            }
        }

        private void InitializeMongo()
        {
            try
            {
                string mongDBString;
                if (isProd)
                    mongDBString = prodDBString;
                else
                    mongDBString = qaDBString;

                mongoClient = new MongoClient(mongDBString);
                mongoServer = mongoClient.GetServer();
                statsDB = mongoServer.GetDatabase("statsdb");
                blahsDB = mongoServer.GetDatabase("blahdb");
                usersDB = mongoServer.GetDatabase("userdb");
                infoDB = mongoServer.GetDatabase("infodb");

                // collections
                blahStats = statsDB.GetCollection<BlahStat>("blahstats");
                userStats = statsDB.GetCollection<UserStat>("userstats");
                groupStats = statsDB.GetCollection<GroupStat>("groupstats");
                systemStats = statsDB.GetCollection<SystemStat>("systemstats");
                rawActivity = statsDB.GetCollection<ActivityRecord>("rawactivity");
                blahsCol = blahsDB.GetCollection<SimpleBlah>("blahs");
                usersCol = usersDB.GetCollection<SimpleUser>("users");
                userBlahStats = statsDB.GetCollection<UserBlahStat>("userblahstats");
                whatsNewCol = usersDB.GetCollection<WhatsNewInfo>("whatsNew");

                blahInfoCol = infoDB.GetCollection("blahInfo");
                userGroupInfoCol = infoDB.GetCollection("userGroupInfo");
            }
            catch(Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

        }


        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();
            InitializeActivityQueue();
            InitializeMongo();

            Trace.TraceInformation("HeadActivityQueue has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("HeadActivityQueue is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("HeadActivityQueue has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                activityQueue.FetchAttributes();
                if (activityQueue.ApproximateMessageCount > 0)
                {
                    Trace.TraceInformation("processing " + activityQueue.ApproximateMessageCount.ToString() + " messages");
                    await Task.Run(() =>
                        {
                            ProcessNextMessages();
                        }
                    );
                }
                else await Task.Delay(1000);
                
            }
        }

        private void ProcessNextMessages()
        {
            foreach (CloudQueueMessage message in activityQueue.GetMessages(20, TimeSpan.FromMinutes(5)))
            {
                String jsonString = message.AsString;
                Trace.TraceInformation("dequeued:  " + jsonString);
                BsonDocument curDoc = BsonDocument.Parse(jsonString);
                BsonValue msgType;
                bool handled = false;

                if (curDoc.TryGetValue("t", out msgType))
                {
                    ActivityType curType = (ActivityType)msgType.AsInt32;
                    switch (curType)
                    {
                        case ActivityType.Login:
                            handled = RecordLogin(curDoc);
                            break;
                        case ActivityType.Logout:
                            handled = RecordLogout(curDoc);
                            break;
                        case ActivityType.ViewPost:
                            handled = RecordViewPost(curDoc);
                            break;
                        case ActivityType.OpenPost:
                            handled = RecordOpenPost(curDoc);
                            break;
                        case ActivityType.VotePost:
                            handled = RecordVotePost(curDoc);
                            break;
                        case ActivityType.VotePoll:
                            handled = RecordVotePoll(curDoc);
                            break;
                        case ActivityType.VotePrediction:
                            handled = RecordVotePrediction(curDoc);
                            break;
                        case ActivityType.VoteExpiredPrediction   :
                            handled = RecordExpiredPrediction(curDoc);
                            break;
                        case ActivityType.VoteComment:
                            handled = RecordVoteComment(curDoc);
                            break;
                        case ActivityType.SubmitPost:
                            handled = RecordSubmitPost(curDoc);
                            break;
                        case ActivityType.SubmitComment:
                            handled = RecordSubmitComment(curDoc);
                            break;
                        case ActivityType.FetchedWhatsNew:
                            handled = HandleResetWhatsNew(curDoc);
                            break;
                    }
                }

                if (handled)
                {
                    ActivityRecord record = new ActivityRecord(curDoc);
                    rawActivity.Insert<ActivityRecord>(record);
                    activityQueue.DeleteMessage(message);
                }

            }

        }

        private bool RecordLogin(BsonDocument curDoc)
        {
            bool handled = false;

            try
            {
                String userId;
                BsonValue curVal;

                if (curDoc.TryGetValue("u", out curVal))
                {
                    // set the last login time for the user
                    String dateStr = curDoc.GetValue("c").ToString();
                    DateTime curDate = DateTime.Parse(dateStr);
                    userId = curVal.ToString();
                    var query = Query<SimpleUser>.EQ(e => e.Id, ObjectId.Parse(userId));
                    IMongoUpdate update = Update<SimpleUser>.Set(e => e.ll, curDate);
                    usersCol.Update(query, update, UpdateFlags.Upsert);

                    // update logins for the system
                    query = Query.And(Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                    update = Update<SystemStat>.Inc(e => e.L, 1);
                    systemStats.Update(query, update, UpdateFlags.Upsert);
                }


                handled = true;
            }
            catch (Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

            return handled;
        }

        
        private bool RecordLogout(BsonDocument curDoc)
        {
            bool handled = false;

            try
            {
                // for now we do nothing with these..
                handled = true;
            }
            catch (Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

            return handled;
        }

        private bool IncrementProperty(BsonDocument curDoc, string propName, string whatsNewName)
        {
            bool handled = false;

            try
            {
                String dateStr = curDoc.GetValue("c").ToString();
                DateTime curDate = DateTime.Parse(dateStr);
                String objectId = curDoc.GetValue("o").ToString();
                String userId;
                BsonValue curVal;

                if (curDoc.TryGetValue("u", out curVal))
                    userId = curVal.ToString();
                else
                    userId = "0";


                // blah has a value
                var query = Query.And(Query.EQ("B", ObjectId.Parse(objectId)), Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                IMongoUpdate update = Update.Inc(propName, 1);
                blahStats.Update(query, update, UpdateFlags.Upsert);

                // blahInfo has a value
                query = Query.EQ("_id", ObjectId.Parse(objectId));
                update = Update.Inc("N." + propName, 1);
                blahInfoCol.Update(query, update);

                // user has a value
                query = Query.And(Query.EQ("U", ObjectId.Parse(userId)), Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                update = Update.Inc(propName, 1);
                userStats.Update(query, update, UpdateFlags.Upsert);

                // userblah has a value
                query = Query.And(Query.EQ("B", ObjectId.Parse(objectId)), Query.EQ("U", ObjectId.Parse(userId)), Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                update = Update.Inc(propName, 1);
                userBlahStats.Update(query, update, UpdateFlags.Upsert);

                var ownerQuery = Query<SimpleBlah>.EQ(e => e.Id, ObjectId.Parse(objectId));
                SimpleBlah theBlah = blahsCol.FindOne(ownerQuery);
                if (theBlah != null)
                {
                    // userGroupInfo has a value
                    query = Query.And(Query.EQ("U", theBlah.A), Query.EQ("G", theBlah.G));
                    update = Update.Inc("N." + propName, 1);
                    userGroupInfoCol.Update(query, update);

                    // blah owner's content has a value
                    query = Query.And(Query.EQ("U", theBlah.A), Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                    update = Update.Inc("S." + propName, 1);
                    userStats.Update(query, update, UpdateFlags.Upsert);

                    // owner has something new!
                    if (!String.IsNullOrEmpty(whatsNewName))
                    {
                        query = Query<WhatsNewInfo>.EQ(e => e.U, theBlah.A);
                        WhatsNewInfo curInfo = whatsNewCol.FindOne(query);
                        if (curInfo == null)
                            update = Update.Combine(Update.Set("u", DateTime.Now),
                                Update.Inc(whatsNewName, 1),
                                Update.Set("M", "New activity since " +  DateTime.Now.ToShortDateString()));
                        else
                            update = Update.Combine(Update.Set("u", DateTime.Now),
                                Update.Inc(whatsNewName, 1));
                        whatsNewCol.Update(query, update, UpdateFlags.Upsert);
                    }


                    // group has a value
                    query = Query.And(Query.EQ("G", theBlah.G), Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                    update = Update.Inc(propName, 1);
                    groupStats.Update(query, update, UpdateFlags.Upsert);
                }


                // system has a value
                query = Query.And(Query.EQ("Y", curDate.Year), Query.EQ("M", curDate.Month), Query.EQ("D", curDate.Day));
                update = Update.Inc(propName, 1);
                systemStats.Update(query, update, UpdateFlags.Upsert);

                handled = true;
            }
            catch (Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

            return handled;
        }

        private bool RecordViewPost(BsonDocument curDoc)
        {
            return IncrementProperty(curDoc, "V", "V");
        }

        private bool RecordOpenPost(BsonDocument curDoc)
        {
            return IncrementProperty(curDoc, "O", "O");
        }

        private bool RecordVotePost(BsonDocument curDoc)
        {
            bool isPromote = false;
            BsonValue curVal;

            if (curDoc.TryGetValue("d", out curVal))
            {
                isPromote = (curVal.ToInt64() > 0);

                if (isPromote)
                    return IncrementProperty(curDoc, "P", "P");
                else
                    return IncrementProperty(curDoc, "N", "N");
            }
                
            else
                return true; // bad record

        }

        private bool RecordVotePoll(BsonDocument curDoc)
        {
            bool handled = false;

            try
            {
                // for now we do nothing with these..
                handled = true;
            }
            catch (Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

            return handled;
        }

        private bool RecordVotePrediction(BsonDocument curDoc)
        {
            bool handled = false;

            try
            {
                // for now we do nothing with these..
                handled = true;
            }
            catch (Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

            return handled;
        }

        private bool RecordExpiredPrediction(BsonDocument curDoc)
        {
            bool handled = false;

            try
            {
                // for now we do nothing with these..
                handled = true;
            }
            catch (Exception exp)
            {
                Debug.WriteLine(exp.Message);
            }

            return handled;
        }

        private bool RecordVoteComment(BsonDocument curDoc)
        {
            bool isPromote = false;
            BsonValue curVal;

            if (curDoc.TryGetValue("d", out curVal))
            {
                isPromote = (curVal.ToInt64() > 0);

                if (isPromote)
                    return IncrementProperty(curDoc, "CP", "CP");
                else
                    return IncrementProperty(curDoc, "CN", "CN");
            }

            else
                return true; // bad record
        }


        private bool RecordSubmitPost(BsonDocument curDoc)
        {
            return IncrementProperty(curDoc, "NP", "NP");

        }

        private bool RecordSubmitComment(BsonDocument curDoc)
        {
            return IncrementProperty(curDoc, "C", "C");
        }

        private bool HandleResetWhatsNew(BsonDocument curDoc)
        {
            bool handled = false;
            BsonValue curVal;
            if (curDoc.TryGetValue("u", out curVal))
            {
                string userId = curVal.ToString();
                if (userId != "0")
                {
                    try
                    {
                        WhatsNewInfo newDoc = whatsNewCol.FindOne(Query<WhatsNewInfo>.EQ(e => e.U, ObjectId.Parse(userId)));
                        if (newDoc != null)
                        {
                            newDoc.Clear();
                            whatsNewCol.Save<WhatsNewInfo>(newDoc);
                        }
                        else
                        {
                            newDoc = new WhatsNewInfo();
                            whatsNewCol.Insert<WhatsNewInfo>(newDoc);
                        }
                        handled = true;

                    }
                    catch (Exception exp)
                    {
                        Debug.WriteLine(exp.Message);
                    }
                }
                
            }


            return handled;
        }



    }


}
