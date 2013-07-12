using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data.SqlServerCe;

namespace SearchEngine
{
    class Searchnet
    {
        public SqlCeConnection connection;
        List<int> wordids = new List<int>();
        List<int> urlids = new List<int>();
        List<int> hiddenids = new List<int>();
        List<double> ai = new List<double>();
        List<double> ah = new List<double>();
        List<double> ao = new List<double>();

        List<List<double>> weigthsint = new List<List<double>>();

        List<List<double>> weigthsout = new List<List<double>>();
        
        
        public Searchnet()
        {
            connection = new SqlCeConnection("Data Source=C:\\Users\\Rajendra.lenovo-PC\\Documents\\SearchEngine.sdf;Password=search;Persist Security Info=True");
            try
            {
                connection.Open();
               
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        ~Searchnet()
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

        public void createnettable()
        {
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;

            try
            {
                command.Connection = connection;

                command.CommandText = "drop table hiddennode ;";
                command.ExecuteNonQuery();
                command.CommandText = "drop table wordhidden ;";
                command.ExecuteNonQuery();
                command.CommandText = "drop table hiddenurl ;";
                command.ExecuteNonQuery();
                command.CommandText = "create table hiddennode (create_key nvarchar(200), rowid int PRIMARY KEY IDENTITY );";
                command.ExecuteNonQuery();
                command.CommandText = "create table wordhidden (rowid int PRIMARY KEY IDENTITY, fromid int, toid int, strength float);";
                command.ExecuteNonQuery();
                command.CommandText = "create table hiddenurl (rowid int PRIMARY KEY IDENTITY ,fromid int, toid int, strength float);";
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(command.CommandText);
                Console.WriteLine("Create net table " + e.ToString());
                return;
            }
        
        
        }


        public double getstrength(int fromid, int toid, int table)
        {
            String layer=null;
            double strength=-0.2;
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            command.Connection = connection;
            if (table == 0)
                layer = "wordhidden";
            if (table == 1)
            {
                layer = "hiddenurl";
                strength = 0;
            }
           
            try
            {
                command.CommandText = "select strength from "+ layer+" where fromid="+fromid+ " and toid="+toid+";";
                myReader = command.ExecuteReader();
                if (myReader.Read())
                    strength = double.Parse(myReader["strength"].ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine(command.CommandText);
                Console.WriteLine("Get Strength " + e.ToString());        
            }

            return strength;
        }

        public void setstrength(int fromid, int toid, int table, double strength)
        {
            String layer = null;
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            command.Connection = connection;
            if (table == 0)
                layer = "wordhidden";
            if (table == 1)          
                layer = "hiddenurl";

            try
            {
                command.CommandText = "select strength from " + layer + " where fromid=" + fromid + " and toid=" + toid + ";";
                myReader = command.ExecuteReader();
                if (!myReader.Read())
                {
                    try
                    {
                        command.CommandText = "insert into " + layer + " (fromid,toid,strength) values (" + fromid + "," + toid + "," + strength + ");";
                        command.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(command.CommandText);
                        Console.WriteLine("set Strength " + e.ToString());
                    }

                }
                else
                {
                    try
                    {
                        command.CommandText = "update " + layer + " set strength=" + strength + " where fromid=" + fromid + " and toid=" + toid + ";";
                        command.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(command.CommandText);
                        Console.WriteLine("set Strength " + e.ToString());
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(command.CommandText);
                Console.WriteLine("set Strength " + e.ToString());
            }
           

        
        }

        public void createhiddennode(List<int> words, List<int> urllist)
        {
            String create_key = String.Join("_",words);
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            command.Connection = connection;
            int id=-1;
            if (words.Count > 3)
                return;
            try
            {
                command.CommandText = "select rowid from hiddennode where create_key=\'" + create_key + "\';";
                myReader = command.ExecuteReader();
                if (!myReader.Read())
                {
                    try
                    {
                        command.CommandText = "insert into hiddennode (create_key) values (\'"+create_key+"\');";
                        command.ExecuteNonQuery();
                        command.CommandText = "select rowid from hiddennode where create_key=\'" + create_key + "\';";
                        myReader = command.ExecuteReader();
                        if (myReader.Read())
                            id = int.Parse(myReader["rowid"].ToString());
                        foreach (var url in urllist)
                            setstrength(id, url, 1,1.0);
                        foreach (var word in words)
                            setstrength(word, id, 0, 1.0 / words.Count);
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(command.CommandText);
                        Console.WriteLine("create hidden node " + e.ToString());
                        
                    }
                
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(command.CommandText);
                Console.WriteLine("create hidden node  " + e.ToString());
            }
        }

        public List<int> getallhiddenids(List<int> words, List<int> urls)
        {
            List<int> rowids = new List<int>();
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            command.Connection = connection;
            int key = -1;

            foreach(var wordid in words)
            {
                try
                {
                    command.CommandText = "select toid from wordhidden where fromid=" +wordid +";";
                    myReader = command.ExecuteReader();
                    while (myReader.Read())
                    {
                        key = int.Parse(myReader["toid"].ToString());
                        if (!rowids.Contains(key))
                            rowids.Add(key);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(command.CommandText);
                    Console.WriteLine("Get all hidden " + e.ToString());
                }
            }
            foreach (var urlid in urls)
            {
                try
                {
                    command.CommandText = "select fromid from wordhidden where toid=" + urlid + ";";
                    myReader = command.ExecuteReader();
                    while (myReader.Read())
                    {
                        key = int.Parse(myReader["fromid"].ToString());
                        if (!rowids.Contains(key))
                            rowids.Add(key);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(command.CommandText);
                    Console.WriteLine("Get all hidden " + e.ToString());
                }
            }

            return rowids;
        }

        public void setupnetwork(List<int> words, List<int> urls)
        {
            wordids = words;
            urlids = urls;
            hiddenids = getallhiddenids(wordids, urlids);
         
            foreach (var word in words)
                ai.Add(1.0);
            foreach (var url in urls)
                ao.Add(1.0);
            foreach (var hidden in hiddenids)
                ah.Add(1.0);

            foreach (var word in words)
            {
                List<double> strength = new List<double>();
                foreach (var hidden in hiddenids)
                {
                    strength.Add(getstrength(word, hidden, 0));
                }
                weigthsint.Add(strength);
            }

            foreach (var hidden in hiddenids)
            {
                List<double> strength = new List<double>();
                foreach (var url in urls)
                {
                    strength.Add(getstrength(hidden, url, 1));
                }
                weigthsout.Add(strength);
            }
        }


        public List<double> feedforward()
        {

            for(int i=0;i<wordids.Count;i++)
                ai[i]=1.0;
            //hidden activations

            for(int j=0;j<hiddenids.Count;j++)
            {
            double sum=0.0;
                for(int i=0;i<wordids.Count;i++)
                {
                sum+=ai[i]*weigthsint[i][j];
                }
            ah[j]=Math.Tanh(sum);
            }
           
            // output activations 

            for (int i = 0; i < urlids.Count; i++)
            {
                double sum = 0.0;
                for (int j = 0; j < hiddenids.Count; j++)
                {
                    sum += ah[j] * weigthsout[j][i];
                }
                ao[i] = Math.Tanh(sum);
            }
            return ao;
        }

        public List<double> getresults(List<int> words, List<int> urls)
        {
         createhiddennode(words, urls);
        setupnetwork(words,urls);
        return feedforward();
        }

        public double dtanh(double y)
        {
            return (1 - y * y);
        }

        public void backpropogate(List<int> targets, double N = 0.5)
        {
            List<double> output_delta = new List<double>();
            List<double> hidden_delta = new List<double>();
            double error = 0;
            foreach (var url in urlids)
                output_delta.Add(0);
            foreach (var hidden in hiddenids)
                hidden_delta.Add(0);    

            // calclating delta 
            for (int i = 0; i < urlids.Count; i++)
            {
                error= targets[i] - ao[i];
                output_delta[i] = error * dtanh(ao[i]);
            }

            for (int j = 0; j < hiddenids.Count; j++)
            {
                error = 0.0;
                for (int i = 0; i < urlids.Count; i++)
                {
                    error += output_delta[i] * weigthsout[j][i];
                }
                hidden_delta[j] = dtanh(ah[j]) * error;
            }
            // setting the activations
    
            for (int i = 0; i < hiddenids.Count; i++)
            { 
                for(int j=0;j< urlids.Count;j++)
                    weigthsout[i][j]+=N*(ah[i]*output_delta[j]);
            }
            for (int i = 0; i < wordids.Count; i++)
            {
                for (int j = 0; j < hiddenids.Count; j++)
                    weigthsint[i][j] += N * (ai[i] * hidden_delta[j]);
            }
         
        }

        public void trainquery(List<int> words,List<int> urls,List<int> selected)
        {
            List<int> targets = new List<int>();
            createhiddennode(words, urls);
            setupnetwork(words, urls);
            feedforward();
            foreach (var url in urls)
            {
                if (selected.Contains(url))
                    targets.Add(1);
                else
                    targets.Add(0);
            }
            backpropogate(targets);
            updatedata();
            
        }

        private void updatedata()
        {
            for (int i = 0; i < wordids.Count; i++)
            {
                for (int j = 0; j < hiddenids.Count; j++)
                {
                    setstrength(wordids[i], hiddenids[j], 0, weigthsint[i][j]);
                }
            }

            for (int i = 0; i < urlids.Count; i++)
            {
                for (int j = 0; j < hiddenids.Count; j++)
                {
                    setstrength(hiddenids[j],urlids[i], 1, weigthsout[j][i]);
                }
            }
 
        }


        public void testing()
        {
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            SqlCeDataReader myReader = null;
            command.Connection = connection;
  
            try
            {
                command.CommandText = "select rowid,fromid,toid,strength from wordhidden ;";
                myReader = command.ExecuteReader();
                while (myReader.Read())
                {
                    Console.WriteLine(myReader["rowid"].ToString() + "," + myReader["fromid"].ToString() + "," + myReader["toid"].ToString() + "," + myReader["strength"].ToString());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(command.CommandText);
                Console.WriteLine("Get all hidden " + e.ToString());
            }
        }
    }
}
