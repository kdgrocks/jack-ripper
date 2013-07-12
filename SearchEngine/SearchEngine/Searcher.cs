using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data.SqlServerCe;

namespace SearchEngine
{
    class Searcher :Searchnet
    {
       
        public Searcher()
        {
            connection = new SqlCeConnection("Data Source=C:\\Users\\Rajendra.lenovo-PC\\Documents\\SearchEngine.sdf;Password=search;Persist Security Info=True");
            try
            {
                connection.Open();
                createpagetable();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        ~Searcher()
        {
            try
            {
                connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        
        }

        public void createpagetable()
        {

            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            
            try
            {
                command.Connection = connection;

                command.CommandText = "drop table pagerank ;";
                command.ExecuteNonQuery();
                
                command.CommandText = "create table pagerank (urlid int PRIMARY KEY, score float);";
                command.ExecuteNonQuery();
                command.CommandText = "insert into pagerank select rowid, 1.0 from urllist";
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine("Create page table " + e.ToString());
                return;
            }
        
        }
        public Tuple<List<String>, List<int>> getMatchingRows(string query)
        {
            String[] wordlist = query.Split(' ');
            String fieldlist="w0.urlid";
            String tablelist = "";
            String Clauselist = "";
            int tablenumber=0;
            int wordid = 0;
            string fullquery = null;
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            List<String> row = new List<String>();
            List<int> wordidlist = new List<int>();
 
            foreach(var word in wordlist)
            {
                
                command.Connection = connection;
                try
                {
                    command.CommandText = "select rowid from wordlist where word=\'" + word + "\';";

                    myReader = command.ExecuteReader();
                    if (myReader.Read())
                    {
                        wordid = int.Parse(myReader["rowid"].ToString());
                        wordidlist.Add(wordid);
                    //    Console.WriteLine(wordid);
                        if (tablenumber > 0)
                        {
                            tablelist += ", ";
                            Clauselist += " and ";
                            Clauselist += "w" + (tablenumber - 1) + ".urlid=" + "w" + (tablenumber) + ".urlid and ";
                        }
                        tablelist += "wordlocation w" + tablenumber;
                        fieldlist += ", w"+ tablenumber+".location";
                        Clauselist += "w" + tablenumber + ".wordid=" + wordid;
                    }
                    else
                    {
                        continue;
                    }
                }
                catch (Exception e)
                { 
                    Console.WriteLine("Matching rows error "+ e.ToString());
                }
                tablenumber++;
            }

            fullquery="select "+fieldlist+" from "+tablelist+" where "+Clauselist+";";
          //  Console.WriteLine(fullquery);
            command.CommandText = fullquery;
          
            myReader = command.ExecuteReader();

            while (myReader.Read())
            { 
                String add="";
                for (int i = 0; i <=tablenumber; i++)
                {
                    add+= myReader.GetValue(i).ToString()+",";
                   
                }
                add = add.Remove(add.Length - 1);
                row.Add(add);

            }
            return Tuple.Create(row, wordidlist);
        }

        public String getUrlname(int urlid)
        {
             SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            try
            {
                command.Connection = connection;
                command.CommandText = "select url from urllist where rowid=" + urlid + ";";
                myReader = command.ExecuteReader();
                if (myReader.Read())
                    return myReader["url"].ToString();
                else
                    return null;
            }
            catch (Exception e)
            {
                Console.WriteLine("GeturlName : " + e.ToString());
                return null;
            }
        }

        public Dictionary<int, double> getScoredList(List<int> wordidlist, List<String> rows)
        {
            Dictionary<int, double> totalscores = new Dictionary<int, double>();
          
            // some weight functions needs to be added 
            var weights =new List<Tuple<double,Dictionary<int,double>>>() ;
            weights.Add(Tuple.Create(1.0, frequencyscores(rows)));
            weights.Add(Tuple.Create(1.0,locationscores(rows)));
            weights.Add(Tuple.Create(1.0, pagerank(rows)));

        //  Remove comments if you train the query with neural netowrks 
           weights.Add(Tuple.Create(1.0,nnscores(wordidlist,rows)));


            foreach (var row in rows)
            {
                totalscores[int.Parse(row.Split(',')[0])] = 0.0;

            }
            foreach(var item in weights)
            {
                foreach (var url in totalscores.Keys.ToList())
                {
                    totalscores[url] += item.Item1 * item.Item2[url];
                }
            }
          
            return totalscores;  
        }
        public void queryprocess(String query)
        { 
            var matches = getMatchingRows(query);
            var scores = getScoredList(matches.Item2, matches.Item1);
            var items = from pair in scores
                        orderby pair.Value descending
                        select pair;

            // Display results.
            foreach(var pair in items)
            {
                Console.WriteLine("{1}:   {0}", getUrlname(pair.Key), pair.Value);
            }
        
        }

        public Dictionary<int, double> normalization(Dictionary<int, double> scores,bool smallerbetter=false)
        {
            List<double> values = scores.Values.ToList();

            double dvalue = 0.0001;
            if (smallerbetter)
            {
                double min = values.Min();
                foreach (var key in scores.Keys.ToList())
                {
                    scores[key] = (min / Math.Max(min, scores[key]));
                }
            }
            else
            {
                double max = values.Max();
                if (max == 0.0)
                    max = dvalue;
                foreach (var key in scores.Keys.ToList())
                {
                    scores[key] = (scores[key]) / max;
                }
            }
            return scores;
        }

        public Dictionary<int, double> frequencyscores(List<string> rows)
        {
            Dictionary<int, double> count = new Dictionary<int, double>();
            foreach (var keyvar in rows)
            {
                var key = int.Parse(keyvar.Split(',')[0]);
                if (!count.ContainsKey(key))
                    count[key] = 0;
                count[key]++;
            }
            return normalization(count);
        }

        public Dictionary<int, double> locationscores(List<string> rows)
        {
            Dictionary<int, double> location = new Dictionary<int, double>();
            int key=-1,sum;
            foreach (var row in rows)
            {
                key = int.Parse(row.Split(',')[0]);
                if (!location.ContainsKey(key))
                    location[key] = 100000;
                sum = 0;

                foreach (var ele in row.Split(','))
                {
                    sum += int.Parse(ele);
                }

                if (location[key] > (sum - key))
                    location[key] = sum - key;

            }

            return normalization(location,true);
        }

        public Dictionary<int, double> distancescores(List<string> rows)
        {
            Dictionary<int, double> location = new Dictionary<int, double>();
            int key = -1, sum;
            int length=rows[0].Split(',').Length ;

            if (length<= 2)
            {
                foreach (var row in rows)
                {
                    if (!location.ContainsKey(key))
                        location[key] = 1;
                    return location;
                }
            }

            foreach (var row in rows)
            {
                key = int.Parse(row.Split(',')[0]);
                if (!location.ContainsKey(key))
                    location[key] = 100000;
                sum = 0;

                for(int i=1;i<length-1;i++)
                {
                    sum += Math.Abs(int.Parse(row.Split(',')[i]) - int.Parse(row.Split(',')[i]));
                }

                if (location[key] > (sum - key))
                    location[key] = sum - key;

            }

            return normalization(location, true);
        }
 
        
        public Dictionary<int, double> inboundlinkscores(List<string> rows)
        {
            Dictionary<int, double> inbound = new Dictionary<int, double>();
            List<int> urllist=new List<int>();
            int key = -1;
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader  myReader=null;
          
            foreach (var row in rows)
            {
                key = int.Parse(row.Split(',')[0]);
                if (!inbound.ContainsKey(key))
                {
                    try
                    {
                        command.Connection = connection;
                        command.CommandText = "select count(*) from link where toid=" + key + ";";
                        myReader = command.ExecuteReader();
                        if (myReader.Read())
                            inbound[key] = int.Parse(myReader.GetValue(0).ToString());
                        else
                            inbound[key] = 0;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Inbound Link :" + e.ToString());
                    }
                }
            }
            return normalization(inbound);
        }

        public void pagerankcalucalte(int iteration=20)
        {
         
            List<int> urlist = new List<int>();
            List<int> fromlist = new List<int>();
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null,myReader1 = null, myReader2 = null;

            try
            {
                command.Connection = connection;
                command.CommandText = "select rowid from urllist ;";
                myReader = command.ExecuteReader();
                while (myReader.Read())
                {
                    if (!urlist.Contains(int.Parse(myReader["rowid"].ToString())))
                        urlist.Add(int.Parse(myReader["rowid"].ToString()));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("PageRank URLllist: " + e.ToString());
            }

            Console.WriteLine(urlist.Count);
            while (iteration > 0)
            {
               
                foreach (var url in urlist)
                {
                    try
                    {
                        double rank = 0.15;
                           
                        command.CommandText = "select fromid from link where toid="+url+" ;";
                        myReader1 = command.ExecuteReader();
                        while (myReader1.Read())
                        {
                            var fromid = int.Parse(myReader1["fromid"].ToString());
                            int count = 0;
                            try
                            {

                                command.CommandText = "select Count(*) from link where fromid=" + fromid+ " ;";
                                myReader2 = command.ExecuteReader();
                                
                               if(myReader2.Read())
                                {
                                    count= int.Parse(myReader2.GetValue(0).ToString());
                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("PageRank COunt : " + e.ToString());
                            }
                            try
                            {

                                command.CommandText = "select score from pagerank where urlid=" + fromid + " ;";
                                myReader2 = command.ExecuteReader();

                                if (myReader2.Read())
                                {
                                    rank =rank+0.85* (float.Parse(myReader2.GetValue(0).ToString())/count);

                                }
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("PageRank :Select score " + e.ToString());
                            }
                                                  
                        }
                        try
                        {

                            command.CommandText = "update pagerank set score=" + (float)rank + " where urlid=" + url + " ;";
                            command.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("PageRank :update score " + e.ToString());
                        }

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("PageRank : " + e.ToString());
                    }
                
                }
                iteration--;
            }
        }

        public Dictionary<int, double> pagerank(List<String> rows)
        {
             Dictionary<int, double> pageranking=new Dictionary<int,double>();
            List<int> urlist = new List<int>();
            List<int> fromlist = new List<int>();
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            int key = -1;
            foreach (var row in rows)
            {
                key = int.Parse(row.Split(',')[0]);
                if (!pageranking.ContainsKey(key))
                {
                    try
                    {
                        command.Connection = connection;
                        command.CommandText = "select score from pagerank where urlid=" + key + ";";
                        myReader = command.ExecuteReader();
                        if (myReader.Read())
                            pageranking.Add(key, (float.Parse(myReader["score"].ToString())));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("PageRank urllist: " + e.ToString());
                    }
                }
            }
            return normalization(pageranking);
        }


        public Dictionary<int, double> nnscores(List<int> wordidlist, List<String> rows)
        {
            Dictionary<int, double> nscore = new Dictionary<int, double>();
            List<int> urllist = new List<int>();
            List<double> result = new List<double>();
            int key = -1;
            int length = rows[0].Split(',').Length;
            foreach (var row in rows)
            {
                key = int.Parse(row.Split(',')[0]);
                if (!urllist.Contains(key))
                    urllist.Add(key);
            }
            result = getresults(wordidlist, urllist);
            for (int i = 0; i < urllist.Count; i++)
            {
                nscore[urllist[i]] = result[i];
            }
            return normalization(nscore);
        }
        public void Searchtesting()
        {
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            try
            {
                command.Connection = connection;
                command.CommandText = "select urlid,score from pagerank order by score desc;";
                myReader = command.ExecuteReader();
                while (myReader.Read())
                    Console.WriteLine(myReader["score"].ToString() + "," + myReader["urlid"].ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine("PageRank urllist: " + e.ToString());
            }
        }

    }
}
