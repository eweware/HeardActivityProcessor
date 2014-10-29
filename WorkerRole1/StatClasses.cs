using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace StatsWorker
{

    [BsonIgnoreExtraElements]
    class SimpleBlah
    {
        public ObjectId Id { get; set; }
        public string G { get; set; }
        public string A { get; set; }
    }

    [BsonIgnoreExtraElements]
    class SimpleUser
    {
        public ObjectId Id { get; set; }
        public DateTime LL { get; set; }
    }

    [BsonIgnoreExtraElements]
    class WhatsNewInfo
    {
        public ObjectId Id { get; set; }
        public string U { get; set; }
        public long newComments { get; set; }
        public long newOpens { get; set; }
        public long newUpVotes { get; set; }
        public long newDownVotes { get; set; }
        public long newCommentUpVotes { get; set; }
        public long newCommentDownVotes { get; set; }
        public long newViews { get; set; }

        public DateTime lastUpdate { get; set; }

        public string message { get; set; }

        public WhatsNewInfo()
        {
            U = "";
            Clear();
        }

        public void Clear()
        {
            newComments = 0;
            newOpens = 0;
            newUpVotes = 0;
            newDownVotes = 0;
            newCommentUpVotes = 0;
            newCommentDownVotes = 0;
            newViews = 0;
            lastUpdate = DateTime.Now;
            message = "What's new for you since " + lastUpdate.ToShortDateString();
        }
    }


    enum ActivityType
    {
        Login = 1,
        Logout = 2,
        ViewPost = 3,
        OpenPost = 4,
        VotePost = 5,
        VotePoll = 6,
        VotePrediction = 7,
        VoteExpiredPrediction = 8,
        VoteComment = 9,
        SubmitPost = 10,
        SubmitComment = 11,
        FetchedWhatsNew = 12
    };

    class BaseStat
    {

        public long openCount { get; set; }
        public long commentCount { get; set; }
        public long viewCount { get; set; }
        public long upVoteCount { get; set; }
        public long downVoteCount { get; set; }
        public long commentUpVoteCount { get; set; }
        public long commentDownVoteCount { get; set; }

        public BaseStat()
        {
            openCount = 0;
            commentCount = 0;
            viewCount = 0;
            upVoteCount = 0;
            downVoteCount = 0;
            commentUpVoteCount = 0;
            commentDownVoteCount = 0;
        }
    }
    
    class DateBase : BaseStat
    {
        public ObjectId Id { get; set; }
        public int year { get; set; }
        public int month { get; set; }
        public int day { get; set; }

        public DateBase()
        {
            year = 0;
            month = 0;
            day = 0;
        }
    }


    class UserStat : DateBase
    { 
        public string userId { get; set; }

        public long postCount { get; set; }
        public BaseStat contentStats { get; set; }

        public UserStat ()
        {
            userId = "";
            postCount = 0;
        }

    }

    class BlahStat : DateBase 
    {
        public string blahId { get; set; }


        public BlahStat()
        {
            blahId = "";
        }
    }

    class UserBlahStat : DateBase
    {
        public string blahId { get; set; }
        public string userId { get; set; }


        public UserBlahStat()
        {
            blahId = "";
            userId = "";
        }
    }

    class GroupStat : DateBase
    {
        public string groupId { get; set; }
        public long postCount { get; set; }

        public GroupStat()
        {
            groupId = "";
        }
    }

    class SystemStat : DateBase
    {
        public long loginCount { get; set; }
        public long postCount { get; set; }
    }

    class ActivityRecord
    {
        public ObjectId Id { get; set; }
        public DateTime c { get; set; }

        public string u { get; set; }

        public string o { get; set; }
        public string d { get; set; }

        public int t { get; set; }

        public ActivityRecord()
        { }

        public ActivityRecord(BsonDocument curDoc)
        {
            c = DateTime.Parse(curDoc.GetValue("c").ToString());
            t = curDoc.GetValue("t").ToInt32();
            
            BsonValue curVal;
            if (curDoc.TryGetValue("o", out curVal))
                o = curVal.ToString();
            else
                o = "";

            if (curDoc.TryGetValue("u", out curVal))
                u = curVal.ToString();
            else
                u = "";

            if (curDoc.TryGetValue("d", out curVal))
                d = curVal.ToString();
            else
                d = "";

        }

    }
}
