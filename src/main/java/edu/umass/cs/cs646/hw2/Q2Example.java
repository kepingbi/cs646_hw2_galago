package edu.umass.cs.cs646.hw2;

import edu.umass.cs.cs646.utils.EvalUtils;
import edu.umass.cs.cs646.utils.GalagoSearchUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tokenize.Tokenizer;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Q2Example {
	
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
			
			GalagoSearchUtils.BestMatchSearch daat = new GalagoSearchUtils.DocAtATime();
			GalagoSearchUtils.BestMatchSearch taat2 = new GalagoSearchUtils.TermAtATime2();
			
			/* Just use RawTF and IDF for this experiment. */
			GalagoSearchUtils.DocumentDependentWeight dd_tf = new GalagoSearchUtils.RawTF();
			GalagoSearchUtils.DocumentIndependentWeight di_idf = new GalagoSearchUtils.IDF();
			
			int n = 10;
			
			System.out.printf( "%-10s%15s%15s\n", "round", "DocAtATime", "TermAtATime2" );
			
			/* Add the BestMatchSearch implementations here. */
			GalagoSearchUtils.BestMatchSearch[] searches = new GalagoSearchUtils.BestMatchSearch[]{
					daat, taat2
			};
			double[][] times = new double[searches.length][n];
			List<Integer> sequence = new ArrayList<>();
			for ( int ix = 0; ix < searches.length; ix++ ) {
				sequence.add( ix );
			}
			
			for ( int round = -1; round < n; round++ ) {
				
				// let's shuffle the sequence in each round to make it fair
				Collections.shuffle( sequence );
				for ( int seq : sequence ) {
					if ( round >= 0 ) {
						times[seq][round] = System.nanoTime();
					}
					for ( String qid : queries.keySet() ) {
						Tokenizer tokenizer = retrieval.getTokenizer();
						List<String> terms = tokenizer.tokenize( queries.get( qid ) ).terms;
						searches[seq].search( index, posting, retrieval, terms, n, di_idf, dd_tf );
					}
					if ( round >= 0 ) {
						times[seq][round] = ( System.nanoTime() - times[seq][round] ) / 1000000;
					}
				}
				
				if ( round >= 0 ) {
					System.out.printf( "%-10d", ( round + 1 ) );
					for ( int ix = 0; ix < searches.length; ix++ ) {
						System.out.printf( "%13.0fms", times[ix][round] );
					}
					System.out.println();
				}
			}
			
			System.out.printf( "%-10s", "mean" );
			for ( int ix = 0; ix < searches.length; ix++ ) {
				System.out.printf( "%13.0fms", StatUtils.mean( times[ix] ) );
			}
			System.out.println();
			
			System.out.printf( "%-10s", "std.dev" );
			for ( int ix = 0; ix < searches.length; ix++ ) {
				System.out.printf( "%13.0fms", Math.pow( StatUtils.variance( times[ix] ), 0.5 ) );
			}
			System.out.println();
			
			retrieval.close();
			posting.close();
			index.close();
			
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}
	
}
