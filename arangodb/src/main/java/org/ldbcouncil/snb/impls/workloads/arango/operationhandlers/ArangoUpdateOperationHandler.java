package org.ldbcouncil.snb.impls.workloads.arango.operationhandlers;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcNoResult;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.UpdateOperationHandler;

import java.nio.file.AccessMode;
import java.util.Map;


//public abstract class ArangoUpdateOperationHandler<TOperation extends Operation<LdbcNoResult>>
//        implements UpdateOperationHandler<TOperation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState>
//{
//    @Override
//    public String getQueryString(org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state, TOperation operation )
//    {
//        return null;
//    }
//
//    public abstract Map<String, Object> getParameters(TOperation operation );
//
//
//    @Override
//    public void executeOperation( TOperation operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
//                                  ResultReporter resultReporter ) throws DbException
//    {
//        String query = getQueryString(state, operation);
//        final Map<String, Object> parameters = getParameters( operation );
//
//        final SessionConfig config = SessionConfig.builder().withDefaultAccessMode( AccessMode.WRITE ).build();
//        final ArangoDatabase db = state.getDatabase();
//        try
//        {
//            final Result result = db.run( query, parameters );
//            result.consume();
//        }
//        catch ( Exception e )
//        {
//            throw new DbException( e );
//        }
//
//        resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
//    }
//}
