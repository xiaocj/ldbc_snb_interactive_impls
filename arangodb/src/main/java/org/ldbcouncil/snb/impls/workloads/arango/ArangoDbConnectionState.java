package org.ldbcouncil.snb.impls.workloads.arango;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import org.ldbcouncil.snb.impls.workloads.BaseDbConnectionState;
import org.ldbcouncil.snb.impls.workloads.QueryStore;

import java.io.IOException;
import java.util.Map;

public class ArangoDbConnectionState<TDbQueryStore extends QueryStore> extends BaseDbConnectionState<TDbQueryStore>
{
    protected final ArangoDB driver;
    private ArangoDatabase db;

    public ArangoDbConnectionState(Map<String, String> properties, TDbQueryStore store ) {
        super(properties, store);

        final String endpointURI = properties.get( "endpoint" );
        final String username = properties.get( "user" );
        final String password = properties.get( "password" );
        final String graphName = properties.get("dbName");

        driver = new ArangoDB.Builder()
                .host(endpointURI, 8529)
                .maxConnections(8)
                .user(username)
                .password(password)
                .build();

        this.db = driver.db(DbName.of(graphName));
    }

    public ArangoDatabase getDatabase() {
        return db;
    }

    @Override
    public void close() throws IOException
    {
        driver.shutdown();
    }
}
