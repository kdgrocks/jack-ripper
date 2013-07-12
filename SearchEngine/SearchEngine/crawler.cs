using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Text.RegularExpressions;
using System.IO;
using HtmlAgilityPack;

namespace SearchEngine
{
    class crawler
    {
        public SqlCeConnection connection;
        public List<String> ignoreWords = new List<string> {"the","of","to","and","a","in","is","it" };
  
        
        public crawler()
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
            createindexes();
        }


       ~crawler()
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


       public void createindexes()
       {

           SqlCeCommand command = new SqlCeCommand();
           command.CommandType = System.Data.CommandType.Text;
           command.Connection = connection;
           try
           {
               command.CommandText = "CREATE TABLE urllist (url nvarchar(200), rowid int PRIMARY KEY IDENTITY ); ";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("URL list problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE TABLE wordlist (word nvarchar(200), rowid int PRIMARY KEY IDENTITY); ";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("word list problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE TABLE wordlocation (urlid int, wordid int,location nvarchar(200),FOREIGN KEY (urlid) REFERENCES urllist(rowid),FOREIGN KEY (wordid) REFERENCES wordlist(rowid)); ";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("WORDLocation Problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE TABLE link (rowid int PRIMARY KEY IDENTITY,fromid int,toid int,FOREIGN KEY (fromid) REFERENCES urllist(rowid),FOREIGN KEY (toid) REFERENCES urllist(rowid)); ";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("LINK problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE TABLE linkwords (wordid int,linkid int,FOREIGN KEY (wordid) REFERENCES wordlist(rowid),FOREIGN KEY (linkid) REFERENCES link(rowid)); ";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("Link Words problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE INDEX wordidx ON wordlist (word)";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("INdex word problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE INDEX urlidx ON urllist (url)";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("URL INdex problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE INDEX wordurlidx ON wordlocation(wordid)";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("Wordid index problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE INDEX urltoidx ON link(toid)";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("Toid index problem" + e.ToString());
           }
           try
           {
               command.CommandText = "CREATE INDEX urlfromidx ON link(fromid)";
               command.ExecuteNonQuery();
           }
           catch (Exception e)
           {
               Console.WriteLine("fromid  problem" + e.ToString());
           }
       }

        public List<String> crawl(List<String> pages, int depth = 2)
        {
            
            for (int i=0;i<depth;i++)
            {
                List<String> newpages = new List<string>();
                foreach(var vpage in pages)
                {
                    var page=vpage.Replace("'",string.Empty);
                    Uri pageurl = new Uri(page);
                    Console.WriteLine(page);
                    var webGet = new HtmlWeb();
                    var doc = webGet.Load(page);
                    if (doc == null) 
                    {
                        Console.WriteLine("Not able to open - " + page);
                        continue;
                    } 
                    AddtoIndex(page,doc);
                   var linksOnPage = from lnks in doc.DocumentNode.Descendants()
                                      where lnks.Name == "a" && 
                                      lnks.Attributes["href"] != null    
                                      select lnks.Attributes["href"].Value;
                  foreach (var link in linksOnPage)
                   {
                      
                       var url = new Uri(pageurl, link);
                       addLinkRef(page, (url.ToString()).Replace("'", string.Empty));
                       newpages.Add(url.ToString().Replace("'", string.Empty));
                   }
                }
                pages = newpages;
                Console.WriteLine(pages.Count);
     
            }
        return pages;
        }


        public void addLinkRef(String from, String to)
        { 
        
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.Connection = connection;
            SqlCeDataReader myReader = null;
            string fromid,toid;
            fromid = getEntryId("urllist","url",from);
            toid = getEntryId("urllist", "url", to);
            command.CommandText = "select * from link where fromid=" + int.Parse(fromid) + " AND toid="+int.Parse(toid)+";";
            //   Console.WriteLine(command.CommandText);
          //  Console.WriteLine(to);
            myReader = command.ExecuteReader();
            if (myReader.Read())
                return ;

            try
            {
                command.CommandText = "insert into link(fromid,toid) values (" + fromid + "," + toid + ");";
                command.ExecuteNonQuery();

            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

        }
        public void AddtoIndex(String url,HtmlDocument doc)
        {
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.Connection = connection;
            string text; 
          
            Console.WriteLine("Indexing -- {0}"+url);
            
            if (isIndexed(url)) return;
            
            text = getTextonly(url,doc);
            
            string[] words = seperateWords(text);

            string urlid=getEntryId("urllist","url",url);

            int i=0,wordid;
            foreach (var word in words)
            {
                if (ignoreWords.Contains(word)) continue;
                
                int.TryParse(getEntryId("wordlist","word",word), out wordid);

                try
                {
                    command.CommandText = "insert into wordlocation(urlid,wordid,location) values (\'" + urlid + "\',\'" + wordid + "\',\'" + i + "\');";
                    command.ExecuteNonQuery();

                }
                catch(Exception e)
                {
                    Console.WriteLine("Add Index " +e.ToString());
                }
                i++;
            }


        }



        public string getTextonly(String url,HtmlDocument doc)
        {
            foreach (var script in doc.DocumentNode.Descendants("script").ToArray())
                script.Remove();
            foreach (var style in doc.DocumentNode.Descendants("style").ToArray())
                style.Remove();
            foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//text()"))
            {
                if(node.InnerText!="" || node.InnerText!=null)
                url = url + node.InnerText;
            }
            url = url.Trim();
            url=Regex.Replace(url, @"\s+", " ");
            url.ToLower();
            //Console.WriteLine(url.Trim());

            return url;
        }

        public string[] seperateWords(String text)
        {
            string[] wordlist= Regex.Split(text, @"\W");

            return wordlist;
        }

        public string getEntryId(String table, String field, String value, bool current = true)
        {
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.Connection = connection;
            SqlCeDataReader myReader = null;
            string rowid=null;
            try
            {
                command.CommandText = "Select rowid from " + table + " where " + field + "=\'" + value + "\';";
               
                myReader = command.ExecuteReader();

                if (myReader.Read() == false)
                {
                    try
                    {
                        command.CommandText = "insert into " + table + " (" + field + ") values " + "(\'" + value + "\');";

                        command.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Inserting error " + command.CommandText);
                        Console.WriteLine(e.ToString());
                    }

                    command.CommandText = "Select rowid from " + table + " where " + field + "=\'" + value + "\';";
                    myReader = command.ExecuteReader();

                    //          Console.WriteLine(myReader.Read());  
                    if (myReader.Read())
                        rowid = (myReader["rowid"].ToString());
                    else
                        Console.WriteLine("Some problem");
                }
                else
                {
                    rowid = (myReader["rowid"].ToString());
                
                }
               // Console.WriteLine(rowid);
                myReader.Close();
                return rowid;
            }

            catch(Exception e )
            {
                Console.WriteLine(command.CommandText);
                Console.WriteLine("Get EntryID!! "+table +" " + field +" "+value +e.ToString());
                return null;
            }

        }


        public bool isIndexed(string url)
        {
            SqlCeCommand command = new SqlCeCommand();
            command.CommandType = System.Data.CommandType.Text;
            command.Connection = connection;
            SqlCeDataReader myReader = null;
            try
            {
                command.CommandText = "Select rowid from urllist where url=\'" + url + "\';";
                myReader = command.ExecuteReader();
                if (myReader.Read())
                {
                    command.CommandText = "Select * from wordlocation where urlid=" +myReader["rowid"]+ ";";
                  //  Console.WriteLine(command.CommandText);
                    myReader = command.ExecuteReader();
                    if (myReader.Read())
                        return true;
                }
                return false;
            }
            catch(Exception e)
            {
            Console.WriteLine("isIndex error "+url+e.ToString());
             return false;
            }
        
        }
   
    
    
    
    }

}
