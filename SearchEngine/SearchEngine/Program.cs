using System;
using System.Collections.Generic;

namespace SearchEngine
{
    class Program
    {
       
        static void Main(string[] args)
        {
             // crawler searchengine = new crawler();
            // List<String> pages = new List<string> { "http://kiwitobes.com/wiki/Perl.html" };
           //  searchengine.crawl(pages);

            Searcher ranker = new Searcher();
            ranker.queryprocess("functional programming");

            //  Searchnet netsearch = new Searchnet();
           //  trainqtuery(list wordids,List urlids, List Selected urls)
          //  netsearch.trainquery(new List<int> { 101, 102 }, new List<int> { 201, 202, 203 }, new List<int> { 201});
 
            Console.WriteLine("Success");
            Console.Read();
        }


   
    }
}
