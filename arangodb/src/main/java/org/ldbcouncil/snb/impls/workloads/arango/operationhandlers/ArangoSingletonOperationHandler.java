package org.ldbcouncil.snb.impls.workloads.arango.operationhandlers;

import com.arangodb.ArangoDatabase;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.SingletonOperationHandler;

import java.text.ParseException;
import java.util.Map;


//public abstract class ArangoSingletonOperationHandler<TOperation extends Operation<TOperationResult>, TOperationResult>
//        implements SingletonOperationHandler<TOperationResult,TOperation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState>
//{
//    public abstract TOperationResult toResult( Record record ) throws ParseException;
//
//    public abstract Map<String, Object> getParameters(org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state, TOperation operation );
//
//    @Override
//    public void executeOperation( TOperation operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
//                                  ResultReporter resultReporter ) throws DbException
//    {
//        String query = getQueryString(state, operation);
//        final Map<String, Object> parameters = getParameters(state, operation );
//
//        ArangoDatabase db = state.getDatabase();
//        try
//        {
//            final ArangoCursor<BaseDocument> cursor = db.query( query, parameters, null,  BaseDocument.class);
//            if ( result.hasNext() )
//            {
//                try
//                {
//                    resultReporter.report( 1, toResult( result.next() ), operation );
//                    final ResultSummary summary = result.consume();
//                }
//                catch ( ParseException e )
//                {
//                    throw new DbException( e );
//                }
//            }
//            else
//            {
//                resultReporter.report( 0, null, operation );
//            }
//        }
//    }
//}
