package edu.umass.cs.cs646.utils;

import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

import java.io.IOException;
import java.util.*;

public class GalagoSearchUtils {
	
	/**
	 * A best-match search interface with replacable dd and di functions.
	 */
	public interface BestMatchSearch {
		List<SearchResult> search( DiskIndex index, IndexPartReader posting, Retrieval retrieval, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws Exception;
	}
	
	/**
	 * An interface for implementing document independent term weight (such as IDF).
	 * This score should only be computed once for each posting list.
	 */
	public interface DocumentIndependentWeight {
		double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, String term );
	}
	
	/**
	 * An interface for implementing document dependent term weight (such as TF).
	 * This score should be computed for each entry of the posting list.
	 */
	public interface DocumentDependentWeight {
		double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, CountIterator iterator, ScoringContext sc, String term );
	}
	
	/**
	 * Uniform term weight -- all terms are equally important.
	 */
	public static final class Uniform implements DocumentIndependentWeight {
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, String term ) {
			return 1;
		}
	}
	
	/**
	 * The original IDF with 0.5 smoothing.
	 */
	public static final class IDF implements DocumentIndependentWeight {
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, String term ) {
			double N = fieldStats.documentCount;
			double n = termStats.nodeDocumentCount;
			return (float) Math.log( ( N + 0.5 ) / ( n + 0.5 ) );
		}
	}
	
	/**
	 * Binary TF function.
	 */
	public static final class BinTF implements DocumentDependentWeight {
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, CountIterator iterator, ScoringContext sc, String term ) {
			return iterator.count( sc ) > 0 ? 1 : 0;
		}
	}
	
	/**
	 * Raw TF function.
	 */
	public static final class RawTF implements DocumentDependentWeight {
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, CountIterator iterator, ScoringContext sc, String term ) {
			return iterator.count( sc );
		}
	}
	
	/**
	 * Implement your LogTF function here.
	 */
	public static final class LogTF implements DocumentDependentWeight {
		
		protected double base;
		
		public LogTF( double base ) {
			this.base = base;
		}
		
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, CountIterator iterator, ScoringContext sc, String term ) {
			return 0;
		}
	}
	
	/**
	 * Implement your RSJ weight here.
	 */
	public static final class RSJ implements DocumentIndependentWeight {
		
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, String term ) {
			return 0;
		}
	}
	
	/**
	 * Implement your BM25TFUnnormalized here.
	 */
	public static final class BM25TFUnnormalized implements DocumentDependentWeight {
		
		double k1;
		
		public BM25TFUnnormalized( double k1 ) {
			this.k1 = k1;
		}
		
		public double getWeight( NodeStatistics termStats, FieldStatistics fieldStats, CountIterator iterator, ScoringContext sc, String term ) {
			return 0;
		}
	}
	
	/**
	 * Implement your term-at-a-time here.
	 */
	public static final class TermAtATime1 implements BestMatchSearch {
		
		public List<SearchResult> search( DiskIndex index, IndexPartReader posting, Retrieval retrieval, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws Exception {
			return null;
		}
	}
	
	public static final class TermAtATime2 implements BestMatchSearch {
		
		private static int merge( CountIterator iterator, int[] docs_old, float[] scores_old, int length_old, int[] docs, float[] scores, double di_weight, NodeStatistics termStats, FieldStatistics fieldStats, String term, DocumentDependentWeight dd ) throws IOException {
			int ix = 0;
			int ix_old = 0;
			ScoringContext sc = new ScoringContext();
			sc.document = iterator.currentCandidate();
			while ( !iterator.isDone() && ix_old < length_old ) {
				int doc2 = docs_old[ix_old];
				if ( sc.document < doc2 ) {
					docs[ix] = (int) sc.document;
					scores[ix] += di_weight * dd.getWeight( termStats, fieldStats, iterator, sc, term );
					ix++;
					iterator.movePast( sc.document );
					sc.document = iterator.currentCandidate();
				} else if ( sc.document > doc2 ) {
					docs[ix] = doc2;
					scores[ix] += scores_old[ix_old];
					ix++;
					ix_old++;
				} else {
					docs[ix] = (int) sc.document;
					scores[ix] += scores_old[ix_old] + di_weight * dd.getWeight( termStats, fieldStats, iterator, sc, term );
					ix++;
					iterator.movePast( sc.document );
					sc.document = iterator.currentCandidate();
					ix_old++;
				}
			}
			while ( !iterator.isDone() ) {
				docs[ix] = (int) sc.document;
				scores[ix] += di_weight * dd.getWeight( termStats, fieldStats, iterator, sc, term );
				ix++;
				iterator.movePast( sc.document );
				sc.document = iterator.currentCandidate();
			}
			while ( ix_old < length_old ) {
				docs[ix] = docs_old[ix_old];
				scores[ix] += scores_old[ix_old];
				ix++;
				ix_old++;
			}
			return ix;
		}
		
		public List<SearchResult> search( DiskIndex index, IndexPartReader posting, Retrieval retrieval, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws Exception {
			
			FieldStatistics fieldStats = retrieval.getCollectionStatistics( StructuredQuery.parse( "#lengths" ) );
			Map<String, NodeStatistics> termStats = new HashMap<>();
			Map<String, Integer> queryterms_df = new TreeMap<>();
			List<String> queryterms_filtered = new ArrayList<>();
			for ( String term : queryterms ) {
				NodeStatistics stats = retrieval.getNodeStatistics( new Node( "counts", term ) );
				termStats.put( term, stats );
				int df = (int) stats.nodeDocumentCount;
				if ( df > 0 ) {
					queryterms_filtered.add( term );
					queryterms_df.put( term, df );
				}
			}
			queryterms = queryterms_filtered;
			
			Collections.sort( queryterms, ( t1, t2 ) -> queryterms_df.get( t1 ) - queryterms_df.get( t2 ) );
			
			int[] docs = new int[0];
			float[] scores = new float[0];
			int length = 0;
			
			for ( String term : queryterms ) {
				int df = queryterms_df.get( term );
				double di_weight = di.getWeight( termStats.get( term ), fieldStats, term );
				int[] docs_old = docs;
				float[] scores_old = scores;
				int length_old = length;
				docs = new int[df + length_old];
				scores = new float[df + length_old];
				CountIterator iterator = (CountIterator) posting.getIterator( new Node( "text", term ) );
				length = merge(
						iterator,
						docs_old, scores_old, length_old,
						docs, scores, di_weight,
						termStats.get( term ), fieldStats, term, dd
				);
			}
			
			// get the top n highest-scored results using a priority queue
			PriorityQueue<SearchResult> pq = new PriorityQueue<>( ( o1, o2 ) -> o1.getScore().compareTo( o2.getScore() ) );
			for ( int ix = 0; ix < length; ix++ ) {
				if ( pq.size() < n ) {
					pq.add( new SearchResult( docs[ix], null, scores[ix] ) );
				} else {
					SearchResult result = pq.peek();
					if ( scores[ix] > result.getScore() ) {
						pq.poll();
						pq.add( new SearchResult( docs[ix], null, scores[ix] ) );
					}
				}
			}
			
			List<SearchResult> results = new ArrayList<>( pq.size() );
			results.addAll( pq );
			Collections.sort( results, ( o1, o2 ) -> o2.getScore().compareTo( o1.getScore() ) );
			return results;
		}
	}
	
	/**
	 * A simple implementation of document-at-a-time.
	 */
	public static final class DocAtATime implements BestMatchSearch {
		
		public List<SearchResult> search( DiskIndex index, IndexPartReader posting, Retrieval retrieval, List<String> queryterms, int n, DocumentIndependentWeight di, DocumentDependentWeight dd ) throws Exception {
			
			PriorityQueue<SearchResult> pq = new PriorityQueue<>( ( o1, o2 ) -> o1.getScore().compareTo( o2.getScore() ) );
			
			FieldStatistics fieldStats = retrieval.getCollectionStatistics( StructuredQuery.parse( "#lengths" ) );
			Map<String, NodeStatistics> termStats = new HashMap<>();
			List<String> queryterms_filtered = new ArrayList<>();
			for ( String term : queryterms ) {
				NodeStatistics stats = retrieval.getNodeStatistics( new Node( "counts", term ) );
				termStats.put( term, stats );
				int df = (int) stats.nodeDocumentCount;
				if ( df > 0 ) {
					queryterms_filtered.add( term );
				}
			}
			queryterms = queryterms_filtered;
			
			ScoringContext[] cursors = new ScoringContext[queryterms.size()];
			double[] di_weights = new double[queryterms.size()];
			CountIterator[] iterators = new CountIterator[queryterms.size()];
			for ( int ix = 0; ix < queryterms.size(); ix++ ) {
				iterators[ix] = (CountIterator) posting.getIterator( new Node( "text", queryterms.get( ix ) ) );
				cursors[ix] = new ScoringContext();
				cursors[ix].document = iterators[ix].currentCandidate();
				di_weights[ix] = di.getWeight( termStats.get( queryterms.get( ix ) ), fieldStats, queryterms.get( ix ) );
			}
			
			while ( true ) {
				long docid_smallest = Long.MAX_VALUE;
				for ( int ix = 0; ix < queryterms.size(); ix++ ) {
					if ( !iterators[ix].isDone() && cursors[ix].document < docid_smallest ) {
						docid_smallest = cursors[ix].document;
					}
					cursors[ix] = new ScoringContext();
					cursors[ix].document = iterators[ix].currentCandidate();
					di_weights[ix] = di.getWeight( termStats.get( queryterms.get( ix ) ), fieldStats, queryterms.get( ix ) );
				}
				if ( docid_smallest == Long.MAX_VALUE ) {
					break;
				}
				double score = 0;
				for ( int ix = 0; ix < cursors.length; ix++ ) {
					if ( cursors[ix].document == docid_smallest ) {
						score += di_weights[ix] * dd.getWeight( termStats.get( queryterms.get( ix ) ), fieldStats, iterators[ix], cursors[ix], queryterms.get( ix ) );
						iterators[ix].movePast( cursors[ix].document );
						cursors[ix].document = iterators[ix].currentCandidate();
					}
				}
				if ( pq.size() < n ) {
					pq.add( new SearchResult( (int) docid_smallest, null, score ) );
				} else {
					SearchResult result = pq.peek();
					if ( score > result.getScore() ) {
						pq.poll();
						pq.add( new SearchResult( (int) docid_smallest, null, score ) );
					}
				}
			}
			
			List<SearchResult> results = new ArrayList<>( pq.size() );
			results.addAll( pq );
			Collections.sort( results, ( o1, o2 ) -> o2.getScore().compareTo( o1.getScore() ) );
			return results;
			
		}
	}
	
}
