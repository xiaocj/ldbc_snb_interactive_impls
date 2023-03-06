package org.ldbcouncil.snb.impls.workloads.arango;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.google.common.collect.ImmutableMap;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;
import org.ldbcouncil.snb.impls.workloads.QueryType;
import org.ldbcouncil.snb.impls.workloads.db.BaseDb;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.ListOperationHandler;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.SingletonOperationHandler;

import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;


public class ArangoDB2 extends BaseDb<ArangoQueryStore>
{

    ArangoQueryStore queryStore;

    static protected Date addDays( Date startDate, int days )
    {
        final Calendar cal = Calendar.getInstance();
        cal.setTime( startDate );
        cal.add( Calendar.DATE, days );
        return cal.getTime();
    }

    static protected Date addMonths( Date startDate, int months )
    {
        final Calendar cal = Calendar.getInstance();
        cal.setTime( startDate );
        cal.add( Calendar.MONTH, months );
        return cal.getTime();
    }

    @Override
    protected void onInit( Map<String, String> properties, LoggingService loggingService ) throws DbException
    {

        dcs = new ArangoDbConnectionState<>(properties, new ArangoQueryStore(properties.get("queryDir")));
        queryStore = new ArangoQueryStore( properties.get( "queryDir" ) );
    }

    // Interactive complex reads
    public static class InteractiveQuery1 implements ListOperationHandler<LdbcQuery1Result,LdbcQuery1, ArangoDbConnectionState>
    {
        @Override
        public void executeOperation(LdbcQuery1 operation, ArangoDbConnectionState state, ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation );

            ArangoDatabase db = state.getDatabase();
            try
            {
                String statement = "";

                // Execute the query and get the results.
                List<LdbcQuery1Result> resultList = new ArrayList<>();
                resultReporter.report( resultList.size(), resultList, operation );
            } catch (final ArangoDBException e) {
                throw new DbException( e );
            }
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery1 operation) {
            return state.getQueryStore().getQuery1Map(operation);
        }

        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery1);
        }
    }

    public static class InteractiveQuery2 implements ListOperationHandler<LdbcQuery2Result,LdbcQuery2, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery2);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery2 operation) {
            return state.getQueryStore().getQuery2Map(operation);
        }

        @Override
        public void executeOperation( LdbcQuery2 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                      ResultReporter resultReporter ) throws DbException
        {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation );
            HashMap p2 = new HashMap();
            parameters.forEach((k, v) -> p2.put(k, v));

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery2Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query( query, p2, null,  BaseDocument.class);
                while ( result.hasNext() ) {
                    BaseDocument doc = result.next();
                    LdbcQuery2Result r = new LdbcQuery2Result(
                            Long.parseLong((String)doc.getAttribute("personId")),
                            (String)doc.getAttribute("personFirstName"),
                            (String)doc.getAttribute("personLastName"),
                            Long.parseLong((String)doc.getAttribute("postOrCommentId")),
                            (String)doc.getAttribute("postOrCommentContent"),
                            (Long)doc.getAttribute("postOrCommentCreationDate"));

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException( e );
            }
        }
    }

    /*
    public static class InteractiveQuery3 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery3,LdbcQuery3Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery3);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery3 operation) {
            return state.getQueryStore().getQuery3Map(operation);
        }

        @Override
        public LdbcQuery3Result toResult( Record record )
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            int xCount = record.get( 3 ).asInt();
            int yCount = record.get( 4 ).asInt();
            int count = record.get( 5 ).asInt();
            return new LdbcQuery3Result(
                    personId,
                    personFirstName,
                    personLastName,
                    xCount,
                    yCount,
                    count );
        }
    }

    public static class InteractiveQuery4 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery4,LdbcQuery4Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery4);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery4 operation) {
            return state.getQueryStore().getQuery4Map(operation);
        }

        @Override
        public LdbcQuery4Result toResult( Record record )
        {
            String tagName = record.get( 0 ).asString();
            int postCount = record.get( 1 ).asInt();
            return new LdbcQuery4Result( tagName, postCount );
        }
    }

    public static class InteractiveQuery5 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery5,LdbcQuery5Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery5);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery5 operation) {
            return state.getQueryStore().getQuery5Map(operation);
        }

        @Override
        public LdbcQuery5Result toResult( Record record )
        {
            String forumTitle = record.get( 0 ).asString();
            int postCount = record.get( 1 ).asInt();
            return new LdbcQuery5Result( forumTitle, postCount );
        }
    }

    public static class InteractiveQuery6 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery6,LdbcQuery6Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery6);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery6 operation) {
            return state.getQueryStore().getQuery6Map(operation);
        }

        @Override
        public LdbcQuery6Result toResult( Record record )
        {
            String tagName = record.get( 0 ).asString();
            int postCount = record.get( 1 ).asInt();
            return new LdbcQuery6Result( tagName, postCount );
        }
    }

    public static class InteractiveQuery7 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery7,LdbcQuery7Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery7);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery7 operation) {
            return state.getQueryStore().getQuery7Map(operation);
        }

        @Override
        public LdbcQuery7Result toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            long likeCreationDate = record.get( 3 ).asLong();
            long messageId = record.get( 4 ).asLong();
            String messageContent = record.get( 5 ).asString();
            int minutesLatency = record.get( 6 ).asInt();
            boolean isNew = record.get( 7 ).asBoolean();
            return new LdbcQuery7Result(
                    personId,
                    personFirstName,
                    personLastName,
                    likeCreationDate,
                    messageId,
                    messageContent,
                    minutesLatency,
                    isNew );
        }
    }

    public static class InteractiveQuery8 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery8,LdbcQuery8Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery8 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery8);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery8 operation) {
            return state.getQueryStore().getQuery8Map(operation);
        }

        @Override
        public LdbcQuery8Result toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            long commentCreationDate = record.get( 3 ).asLong();
            long commentId = record.get( 4 ).asLong();
            String commentContent = record.get( 5 ).asString();
            return new LdbcQuery8Result(
                    personId,
                    personFirstName,
                    personLastName,
                    commentCreationDate,
                    commentId,
                    commentContent );
        }
    }

    public static class InteractiveQuery9 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery9,LdbcQuery9Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery9 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery9);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery9 operation) {
            return state.getQueryStore().getQuery9Map(operation);
        }

        @Override
        public LdbcQuery9Result toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            long messageId = record.get( 3 ).asLong();
            String messageContent = record.get( 4 ).asString();
            long messageCreationDate = record.get( 5 ).asLong();
            return new LdbcQuery9Result(
                    personId,
                    personFirstName,
                    personLastName,
                    messageId,
                    messageContent,
                    messageCreationDate );
        }
    }

    public static class InteractiveQuery10 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery10,LdbcQuery10Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery10 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery10);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery10 operation) {
            return state.getQueryStore().getQuery10Map(operation);
        }

        @Override
        public LdbcQuery10Result toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            int commonInterestScore = record.get( 3 ).asInt();
            String personGender = record.get( 4 ).asString();
            String personCityName = record.get( 5 ).asString();
            return new LdbcQuery10Result(
                    personId,
                    personFirstName,
                    personLastName,
                    commonInterestScore,
                    personGender,
                    personCityName );
        }
    }

    public static class InteractiveQuery11 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery11,LdbcQuery11Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery11 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery11);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery11 operation) {
            return state.getQueryStore().getQuery11Map(operation);
        }

        @Override
        public LdbcQuery11Result toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            String organizationName = record.get( 3 ).asString();
            int organizationWorkFromYear = record.get( 4 ).asInt();
            return new LdbcQuery11Result(
                    personId,
                    personFirstName,
                    personLastName,
                    organizationName,
                    organizationWorkFromYear );
        }
    }

    public static class InteractiveQuery12 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery12,LdbcQuery12Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery12 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery12);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery12 operation) {
            return state.getQueryStore().getQuery12Map(operation);
        }

        @Override
        public LdbcQuery12Result toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String personFirstName = record.get( 1 ).asString();
            String personLastName = record.get( 2 ).asString();
            List<String> tagNames = new ArrayList<>();
            if ( !record.get( 3 ).isNull() )
            {
                tagNames = record.get( 3 ).asList( Value::asString );
            }
            int replyCount = record.get( 4 ).asInt();
            return new LdbcQuery12Result(
                    personId,
                    personFirstName,
                    personLastName,
                    tagNames,
                    replyCount );
        }
    }

    public static class InteractiveQuery13 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoIC13OperationHandler
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery13 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery13);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery13 operation) {
            return state.getQueryStore().getQuery13Map(operation);
        }

        @Override
        public LdbcQuery13Result toResult( Record record )
        {
            return new LdbcQuery13Result( record.get( 0 ).asInt() );
        }
    }

    public static class InteractiveQuery14 extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcQuery14,LdbcQuery14Result>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery14 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery14);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery14 operation) {
            return state.getQueryStore().getQuery14Map(operation);
        }

        @Override
        public LdbcQuery14Result toResult( Record record ) throws ParseException
        {
            List<Long> personIdsInPath = new ArrayList<>();
            if ( !record.get( 0 ).isNull() )
            {
                personIdsInPath = record.get( 0 ).asList( Value::asLong );
            }
            double pathWeight = record.get( 1 ).asDouble();
            return new LdbcQuery14Result(
                    personIdsInPath,
                    pathWeight );
        }
    }

     */

    // Interactive short reads
    public static class ShortQuery1PersonProfile implements SingletonOperationHandler<LdbcShortQuery1PersonProfileResult,LdbcShortQuery1PersonProfile, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery1PersonProfile operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery1);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery1PersonProfile operation) {
            return state.getQueryStore().getShortQuery1PersonProfileMap(operation);
        }

        @Override
        public void executeOperation( LdbcShortQuery1PersonProfile operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                      ResultReporter resultReporter ) throws DbException
        {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation );

            ArangoDatabase db = state.getDatabase();
            try
            {
                final ArangoCursor<BaseDocument> result = db.query( query, parameters, null,  BaseDocument.class);
                if ( result.hasNext() )
                {
                    BaseDocument doc = result.next();
                    resultReporter.report( 1,
                            new LdbcShortQuery1PersonProfileResult(
                                    (String)doc.getAttribute("firstName"),
                                    (String)doc.getAttribute("lastName"),
                                    (Long)doc.getAttribute("birthday"),
                                    (String)doc.getAttribute("locationIP"),
                                    (String)doc.getAttribute("browserUsed"),
                                    Long.decode((String)doc.getAttribute("cityId")),
                                    (String)doc.getAttribute("gender"),
                                    (Long)doc.getAttribute("creationDate")),
                            operation );
                }
                else
                {
                    resultReporter.report( 0, null, operation );
                }
            } catch (final ArangoDBException e) {
                throw new DbException( e );
            }
        }
    }

    public static class ShortQuery2PersonPosts implements ListOperationHandler<LdbcShortQuery2PersonPostsResult,LdbcShortQuery2PersonPosts, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery2PersonPosts operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery2);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery2PersonPosts operation) {
            return state.getQueryStore().getShortQuery2PersonPostsMap(operation);
        }

        @Override
        public void executeOperation( LdbcShortQuery2PersonPosts operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                      ResultReporter resultReporter ) throws DbException
        {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation );

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcShortQuery2PersonPostsResult> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query( query, parameters, null,  BaseDocument.class);
                while ( result.hasNext() ) {

                    BaseDocument doc = result.next();

                    String content = (String)doc.getAttribute("messageContent");
                    if (content == null) {
                        content = (String)doc.getAttribute("messageImageFile");
                    }

                    resultList.add(new LdbcShortQuery2PersonPostsResult(
                            Long.valueOf((String)doc.getAttribute("messageId")),
                            content,
                            (Long)doc.getAttribute("messageCreationDate"),
                            Long.valueOf((String)doc.getAttribute("originalPostId")),
                            Long.valueOf((String)doc.getAttribute("originalPostAuthorId")),
                            (String)doc.getAttribute("originalPostAuthorFirstName"),
                            (String)doc.getAttribute("originalPostAuthorLastName")));

                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException( e );
            }
        }

    }

    /*
    public static class ShortQuery3PersonFriends extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcShortQuery3PersonFriends,LdbcShortQuery3PersonFriendsResult>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery3PersonFriends operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery3);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery3PersonFriends operation) {
            return state.getQueryStore().getShortQuery3PersonFriendsMap(operation);
        }

        @Override
        public LdbcShortQuery3PersonFriendsResult toResult( Record record ) throws ParseException
        {
            long personId = record.get( 0 ).asLong();
            String firstName = record.get( 1 ).asString();
            String lastName = record.get( 2 ).asString();
            long friendshipCreationDate = record.get( 3 ).asLong();
            return new LdbcShortQuery3PersonFriendsResult(
                    personId,
                    firstName,
                    lastName,
                    friendshipCreationDate );
        }
    }

    public static class ShortQuery4MessageContent extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoSingletonOperationHandler<LdbcShortQuery4MessageContent,LdbcShortQuery4MessageContentResult>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery4MessageContent operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery4);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery4MessageContent operation) {
            return state.getQueryStore().getShortQuery4MessageContentMap(operation);
        }

        @Override
        public LdbcShortQuery4MessageContentResult toResult( Record record ) throws ParseException
        {
            // Pay attention, the spec's and the implementation's parameter orders are different.
            long messageCreationDate = record.get( 0 ).asLong();
            String messageContent = record.get( 1 ).asString();
            return new LdbcShortQuery4MessageContentResult(
                    messageContent,
                    messageCreationDate );
        }
    }

    public static class ShortQuery5MessageCreator extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoSingletonOperationHandler<LdbcShortQuery5MessageCreator,LdbcShortQuery5MessageCreatorResult>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery5MessageCreator operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery5);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery5MessageCreator operation) {
            return state.getQueryStore().getShortQuery5MessageCreatorMap(operation);
        }

        @Override
        public LdbcShortQuery5MessageCreatorResult toResult( Record record )
        {
            long personId = record.get( 0 ).asLong();
            String firstName = record.get( 1 ).asString();
            String lastName = record.get( 2 ).asString();
            return new LdbcShortQuery5MessageCreatorResult(
                    personId,
                    firstName,
                    lastName );
        }
    }

    public static class ShortQuery6MessageForum extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoSingletonOperationHandler<LdbcShortQuery6MessageForum,LdbcShortQuery6MessageForumResult>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery6MessageForum operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery6);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery6MessageForum operation) {
            return state.getQueryStore().getShortQuery6MessageForumMap(operation);
        }

        @Override
        public LdbcShortQuery6MessageForumResult toResult( Record record )
        {
            long forumId = record.get( 0 ).asLong();
            String forumTitle = record.get( 1 ).asString();
            long moderatorId = record.get( 2 ).asLong();
            String moderatorFirstName = record.get( 3 ).asString();
            String moderatorLastName = record.get( 4 ).asString();
            return new LdbcShortQuery6MessageForumResult(
                    forumId,
                    forumTitle,
                    moderatorId,
                    moderatorFirstName,
                    moderatorLastName );
        }
    }

    public static class ShortQuery7MessageReplies extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoListOperationHandler<LdbcShortQuery7MessageReplies,LdbcShortQuery7MessageRepliesResult>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery7MessageReplies operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery7);
        }

        @Override
        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery7MessageReplies operation) {
            return state.getQueryStore().getShortQuery7MessageRepliesMap(operation);
        }

        @Override
        public LdbcShortQuery7MessageRepliesResult toResult( Record record ) throws ParseException
        {
            long commentId = record.get( 0 ).asLong();
            String commentContent = record.get( 1 ).asString();
            long commentCreationDate = record.get( 2 ).asLong();
            long replyAuthorId = record.get( 3 ).asLong();
            String replyAuthorFirstName = record.get( 4 ).asString();
            String replyAuthorLastName = record.get( 5 ).asString();
            boolean replyAuthorKnowsOriginalMessageAuthor = record.get( 6 ).asBoolean();
            return new LdbcShortQuery7MessageRepliesResult(
                    commentId,
                    commentContent,
                    commentCreationDate,
                    replyAuthorId,
                    replyAuthorFirstName,
                    replyAuthorLastName,
                    replyAuthorKnowsOriginalMessageAuthor );
        }
    }

    // Interactive inserts

    public static class Update1AddPerson extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate1AddPerson>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate1AddPerson operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate1);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate1AddPerson operation )
        {
            final List<List<Long>> universities =
                    operation.getStudyAt().stream().map( u -> Arrays.asList( u.getOrganizationId(), (long) u.getYear() ) ).collect( Collectors.toList() );
            final List<List<Long>> companies =
                    operation.getWorkAt().stream().map( c -> Arrays.asList( c.getOrganizationId(), (long) c.getYear() ) ).collect( Collectors.toList() );

            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate1AddPerson.PERSON_ID, operation.getPersonId() )
                               .put( LdbcUpdate1AddPerson.PERSON_FIRST_NAME, operation.getPersonFirstName() )
                               .put( LdbcUpdate1AddPerson.PERSON_LAST_NAME, operation.getPersonLastName() )
                               .put( LdbcUpdate1AddPerson.GENDER, operation.getGender() )
                               .put( LdbcUpdate1AddPerson.BIRTHDAY, operation.getBirthday().getTime() )
                               .put( LdbcUpdate1AddPerson.CREATION_DATE, operation.getCreationDate().getTime() )
                               .put( LdbcUpdate1AddPerson.LOCATION_IP, operation.getLocationIp() )
                               .put( LdbcUpdate1AddPerson.BROWSER_USED, operation.getBrowserUsed() )
                               .put( LdbcUpdate1AddPerson.CITY_ID, operation.getCityId() )
                               .put( LdbcUpdate1AddPerson.LANGUAGES, operation.getLanguages() )
                               .put( LdbcUpdate1AddPerson.EMAILS, operation.getEmails() )
                               .put( LdbcUpdate1AddPerson.TAG_IDS, operation.getTagIds() )
                               .put( LdbcUpdate1AddPerson.STUDY_AT, universities )
                               .put( LdbcUpdate1AddPerson.WORK_AT, companies )
                               .build();
        }
    }

    public static class Update2AddPostLike extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate2AddPostLike>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate2AddPostLike operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate2);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate2AddPostLike operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate2AddPostLike.PERSON_ID, operation.getPersonId() )
                               .put( LdbcUpdate2AddPostLike.POST_ID, operation.getPostId() )
                               .put( LdbcUpdate2AddPostLike.CREATION_DATE, operation.getCreationDate().getTime() )
                               .build();
        }
    }

    public static class Update3AddCommentLike extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate3AddCommentLike>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate3AddCommentLike operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate3);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate3AddCommentLike operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate3AddCommentLike.PERSON_ID, operation.getPersonId() )
                               .put( LdbcUpdate3AddCommentLike.COMMENT_ID, operation.getCommentId() )
                               .put( LdbcUpdate3AddCommentLike.CREATION_DATE, operation.getCreationDate().getTime() )
                               .build();
        }
    }

    public static class Update4AddForum extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate4AddForum>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate4AddForum operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate4);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate4AddForum operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate4AddForum.FORUM_ID, operation.getForumId() )
                               .put( LdbcUpdate4AddForum.FORUM_TITLE, operation.getForumTitle() )
                               .put( LdbcUpdate4AddForum.CREATION_DATE, operation.getCreationDate().getTime() )
                               .put( LdbcUpdate4AddForum.MODERATOR_PERSON_ID, operation.getModeratorPersonId() )
                               .put( LdbcUpdate4AddForum.TAG_IDS, operation.getTagIds() )
                               .build();
        }
    }

    public static class Update5AddForumMembership extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate5AddForumMembership>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate5AddForumMembership operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate5);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate5AddForumMembership operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate5AddForumMembership.FORUM_ID, operation.getForumId() )
                               .put( LdbcUpdate5AddForumMembership.PERSON_ID, operation.getPersonId() )
                               .put( LdbcUpdate5AddForumMembership.JOIN_DATE, operation.getJoinDate().getTime() )
                               .build();
        }
    }

    public static class Update6AddPost extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate6AddPost>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate6AddPost operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate6);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate6AddPost operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate6AddPost.POST_ID, operation.getPostId() )
                               .put( LdbcUpdate6AddPost.IMAGE_FILE, operation.getImageFile() )
                               .put( LdbcUpdate6AddPost.CREATION_DATE, operation.getCreationDate().getTime() )
                               .put( LdbcUpdate6AddPost.LOCATION_IP, operation.getLocationIp() )
                               .put( LdbcUpdate6AddPost.BROWSER_USED, operation.getBrowserUsed() )
                               .put( LdbcUpdate6AddPost.LANGUAGE, operation.getLanguage() )
                               .put( LdbcUpdate6AddPost.CONTENT, operation.getContent() )
                               .put( LdbcUpdate6AddPost.LENGTH, operation.getLength() )
                               .put( LdbcUpdate6AddPost.AUTHOR_PERSON_ID, operation.getAuthorPersonId() )
                               .put( LdbcUpdate6AddPost.FORUM_ID, operation.getForumId() )
                               .put( LdbcUpdate6AddPost.COUNTRY_ID, operation.getCountryId() )
                               .put( LdbcUpdate6AddPost.TAG_IDS, operation.getTagIds() )
                               .build();
        }
    }

    public static class Update7AddComment extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate7AddComment>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate7AddComment operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate7);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate7AddComment operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate7AddComment.COMMENT_ID, operation.getCommentId() )
                               .put( LdbcUpdate7AddComment.CREATION_DATE, operation.getCreationDate().getTime() )
                               .put( LdbcUpdate7AddComment.LOCATION_IP, operation.getLocationIp() )
                               .put( LdbcUpdate7AddComment.BROWSER_USED, operation.getBrowserUsed() )
                               .put( LdbcUpdate7AddComment.CONTENT, operation.getContent() )
                               .put( LdbcUpdate7AddComment.LENGTH, operation.getLength() )
                               .put( LdbcUpdate7AddComment.AUTHOR_PERSON_ID, operation.getAuthorPersonId() )
                               .put( LdbcUpdate7AddComment.COUNTRY_ID, operation.getCountryId() )
                               .put( LdbcUpdate7AddComment.REPLY_TO_POST_ID, operation.getReplyToPostId() )
                               .put( LdbcUpdate7AddComment.REPLY_TO_COMMENT_ID, operation.getReplyToCommentId() )
                               .put( LdbcUpdate7AddComment.TAG_IDS, operation.getTagIds() )
                               .build();
        }
    }

    public static class Update8AddFriendship extends org.ldbcouncil.snb.impls.workloads.arango.operationhandlers.ArangoUpdateOperationHandler<LdbcUpdate8AddFriendship>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcUpdate8AddFriendship operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveUpdate8);
        }

        @Override
        public Map<String, Object> getParameters( LdbcUpdate8AddFriendship operation )
        {
            return ImmutableMap.<String, Object>builder()
                               .put( LdbcUpdate8AddFriendship.PERSON1_ID, operation.getPerson1Id() )
                               .put( LdbcUpdate8AddFriendship.PERSON2_ID, operation.getPerson2Id() )
                               .put( LdbcUpdate8AddFriendship.CREATION_DATE, operation.getCreationDate().getTime() )
                               .build();
        }
    }
     */
}
