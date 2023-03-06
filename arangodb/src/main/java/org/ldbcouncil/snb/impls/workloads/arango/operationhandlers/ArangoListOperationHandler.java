package org.ldbcouncil.snb.impls.workloads.arango.operationhandlers;

import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.ListOperationHandler;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


//public abstract class ArangoListOperationHandler<TOperation extends Operation<List<TOperationResult>>, TOperationResult>
//        implements ListOperationHandler<TOperationResult,TOperation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState>
//{
//
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
//        final SessionConfig config = SessionConfig.builder().withDefaultAccessMode( AccessMode.READ ).build();
//        try ( final Session session = state.getSession( config ) )
//        {
//            final List<TOperationResult> results = new ArrayList<>();
//            final Result result = session.run( query, parameters );
//            for ( Record record : result.list() )
//            {
//                try
//                {
//                    results.add( toResult( record ) );
//                }
//                catch ( ParseException e )
//                {
//                    throw new DbException( e );
//                }
//            }
//            result.consume();
//            resultReporter.report( results.size(), results, operation );
//        }
//    }
//}
