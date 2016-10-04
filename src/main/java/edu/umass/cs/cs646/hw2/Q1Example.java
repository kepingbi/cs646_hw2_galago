package edu.umass.cs.cs646.hw2;

import edu.umass.cs.cs646.utils.EvalUtils;
import edu.umass.cs.cs646.utils.GalagoSearchUtils;
import edu.umass.cs.cs646.utils.SearchResult;
import org.apache.commons.math3.stat.StatUtils;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tokenize.Tokenizer;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Q1Example {
	
	public static void main( String[] args ) {
		try {
			
			String pathIndex = "/home/jiepu/Downloads/index_galago_robust04_krovetz"; // change to your own index path
			File pathPosting = new File( new File( pathIndex ), "postings.krovetz" ); // change to your own posting file path
			
			String pathQueries = "/home/jiepu/Downloads/queries_short"; // change it to your query file path
			String pathQrels = "/home/jiepu/Downloads/qrels"; // change it to your qrels file path
			
			DiskIndex index = new DiskIndex( pathIndex );
			IndexPartReader posting = DiskIndex.openIndexPart( pathPosting.getAbsolutePath() );
			Retrieval retrieval = RetrievalFactory.instance( pathIndex );
			
			Map<String, String> queries = EvalUtils.loadQueries( pathQueries );
			Map<String, Set<String>> qrels = EvalUtils.loadQrels( pathQrels );
			
			GalagoSearchUtils.BestMatchSearch search = new GalagoSearchUtils.TermAtATime2();
			
			GalagoSearchUtils.DocumentDependentWeight dd_bintf = new GalagoSearchUtils.BinTF();
			GalagoSearchUtils.DocumentDependentWeight dd_rawtf = new GalagoSearchUtils.RawTF();
			
			GalagoSearchUtils.DocumentIndependentWeight di_uni = new GalagoSearchUtils.Uniform();
			GalagoSearchUtils.DocumentIndependentWeight di_idf = new GalagoSearchUtils.IDF();
			
			int n = 1000;
			
			/* Add your dd functions here. */
			GalagoSearchUtils.DocumentDependentWeight[] dds = new GalagoSearchUtils.DocumentDependentWeight[]{
					dd_bintf, dd_rawtf
			};
			
			/* Add your di functions here. */
			GalagoSearchUtils.DocumentIndependentWeight[] dis = new GalagoSearchUtils.DocumentIndependentWeight[]{
					di_uni, di_idf
			};
			
			/* The names of your di functions here */
			String[] dd_names = new String[]{
					"Bin TF", "Raw TF"
			};
			
			/* The names of your dd functions here. */
			String[] di_names = new String[]{
					"Uniform", "IDF"
			};
			
			double[][][] p10 = new double[dis.length][dds.length][queries.size()];
			double[][][] ap = new double[dis.length][dds.length][queries.size()];
			
			int ix = 0;
			for ( String qid : queries.keySet() ) {
				
				String query = queries.get( qid );
				
				Tokenizer tokenizer = retrieval.getTokenizer();
				List<String> terms = tokenizer.tokenize( query ).terms;
				
				for ( int i = 0; i < dis.length; i++ ) {
					for ( int j = 0; j < dds.length; j++ ) {
						
						List<SearchResult> results = search.search( index, posting, retrieval, terms, n, dis[i], dds[j] );
						SearchResult.dumpDocno( retrieval, results );
						
						p10[i][j][ix] = EvalUtils.precision( results, qrels.get( qid ), 10 );
						ap[i][j][ix] = EvalUtils.avgPrec( results, qrels.get( qid ), n );
					}
				}
				
				ix++;
			}
			
			for ( int i = 0; i < dis.length; i++ ) {
				for ( int j = 0; j < dds.length; j++ ) {
					System.out.printf(
							"%-10s%-25s%10.3f%10.3f\n",
							di_names[i],
							dd_names[j],
							StatUtils.mean( p10[i][j] ),
							StatUtils.mean( ap[i][j] )
					);
				}
			}
			
			retrieval.close();
			posting.close();
			index.close();
			
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}
	
}
