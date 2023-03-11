package org.ldbcouncil.snb.impls.workloads.arango;

import com.arangodb.*;
import com.arangodb.entity.BaseDocument;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.ResultReporter;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.workloads.interactive.*;
import org.ldbcouncil.snb.impls.workloads.QueryType;
import org.ldbcouncil.snb.impls.workloads.arango.converter.ArangoConverter;
import org.ldbcouncil.snb.impls.workloads.db.BaseDb;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.ListOperationHandler;
import org.ldbcouncil.snb.impls.workloads.operationhandlers.SingletonOperationHandler;

import java.util.*;


public class ArangoDB2 extends BaseDb<ArangoQueryStore> {

    ArangoQueryStore queryStore;

    static protected Date addDays(Date startDate, int days) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(startDate);
        cal.add(Calendar.DATE, days);
        return cal.getTime();
    }

    static protected Date addMonths(Date startDate, int months) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(startDate);
        cal.add(Calendar.MONTH, months);
        return cal.getTime();
    }

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {

        dcs = new ArangoDbConnectionState<>(properties, new ArangoQueryStore(properties.get("queryDir")));
        queryStore = new ArangoQueryStore(properties.get("queryDir"));
    }

    // Interactive complex reads
    public static class InteractiveQuery1 implements ListOperationHandler<LdbcQuery1Result, LdbcQuery1, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery1 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery1);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery1 operation) {
            return state.getQueryStore().getQuery1Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery1 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery1Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    Double distance = (Double) doc.getAttribute("distanceFromPerson");
                    LdbcQuery1Result r = new LdbcQuery1Result(
                            Long.parseLong((String) doc.getAttribute("friendId")),
                            (String) doc.getAttribute("friendLastName"),
                            (int) Math.round(distance),
                            (Long) doc.getAttribute("friendBirthday"),
                            (Long) doc.getAttribute("friendCreationDate"),
                            (String) doc.getAttribute("friendGender"),
                            (String) doc.getAttribute("friendBrowserUsed"),
                            (String) doc.getAttribute("friendLocationIp"),
                            new ArrayList<String>(),
                            new ArrayList<String>(),
                            (String) doc.getAttribute("friendCityName"),
                            new ArrayList<LdbcQuery1Result.Organization>(),
                            new ArrayList<LdbcQuery1Result.Organization>()
                    );

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }


    }

    public static class InteractiveQuery2 implements ListOperationHandler<LdbcQuery2Result, LdbcQuery2, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery2 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery2);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery2 operation) {
            return state.getQueryStore().getQuery2Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery2 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery2Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcQuery2Result r = new LdbcQuery2Result(
                            Long.parseLong((String) doc.getAttribute("personId")),
                            (String) doc.getAttribute("personFirstName"),
                            (String) doc.getAttribute("personLastName"),
                            Long.parseLong((String) doc.getAttribute("postOrCommentId")),
                            (String) doc.getAttribute("postOrCommentContent"),
                            (Long) doc.getAttribute("postOrCommentCreationDate"));

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class InteractiveQuery3 implements ListOperationHandler<LdbcQuery3Result, LdbcQuery3, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery3 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery3);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery3 operation) {
            return state.getQueryStore().getQuery3Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery3 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery3Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    Object xCount = doc.getAttribute("xCount");
                    Object yCount = doc.getAttribute("yCount");
                    Double count = (Double) doc.getAttribute("count");

                    LdbcQuery3Result r = new LdbcQuery3Result(
                            Long.parseLong((String) doc.getAttribute("personId")),
                            (String) doc.getAttribute("personFirstName"),
                            (String) doc.getAttribute("personLastName"),
                            (xCount != null) ? (Long) xCount : 0,
                            (yCount != null) ? (Long) yCount : 0,
                            Math.round(count));

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class InteractiveQuery4 implements ListOperationHandler<LdbcQuery4Result, LdbcQuery4, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery4 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery4);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery4 operation) {
            return state.getQueryStore().getQuery4Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery4 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery4Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);

                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    Long postCount = (Long) doc.getAttribute("postCount");
                    LdbcQuery4Result r = new LdbcQuery4Result(
                            ArangoConverter.convertToString(doc.getAttribute("tagName")),
                            postCount.intValue());

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
//                System.out.printf("InteractiveQuery4=%s", e);
                throw new DbException(e);
            }
        }
    }


    public static class InteractiveQuery5 implements ListOperationHandler<LdbcQuery5Result, LdbcQuery5, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery5 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery5);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery5 operation) {
            return state.getQueryStore().getQuery5Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery5 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            // TODO
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery5Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcQuery5Result r = new LdbcQuery5Result(
                            (String) doc.getAttribute("forumTitle"),
                            (Integer) doc.getAttribute("postCount"));

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class InteractiveQuery6 implements ListOperationHandler<LdbcQuery6Result, LdbcQuery6, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery6 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery6);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery6 operation) {
            return state.getQueryStore().getQuery6Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery6 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery6Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    Long postCount = (Long) doc.getAttribute("postCount");
                    LdbcQuery6Result r = new LdbcQuery6Result(
                            ArangoConverter.convertToString(doc.getAttribute("tagName")),
                            postCount.intValue());

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class InteractiveQuery7 implements ListOperationHandler<LdbcQuery7Result, LdbcQuery7, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery7 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery7);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery7 operation) {
            return state.getQueryStore().getQuery7Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery7 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery7Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    Long minutesLatency = Math.round((Double) doc.getAttribute("minutesLatency"));
                    LdbcQuery7Result r = new LdbcQuery7Result(
                            Long.parseLong((String) doc.getAttribute("friendId")),
                            (String) doc.getAttribute("friendFirstName"),
                            (String) doc.getAttribute("friendLastName"),
                            (Long) doc.getAttribute("likesCreationDate"),
                            Long.parseLong((String) doc.getAttribute("messageId")),
                            (String) doc.getAttribute("messageContent"),
                            minutesLatency.intValue(),
                            (Boolean) doc.getAttribute("isNew")
                    );

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class InteractiveQuery8 implements ListOperationHandler<LdbcQuery8Result, LdbcQuery8, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery8 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery8);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery8 operation) {
            return state.getQueryStore().getQuery8Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery8 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);
            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery8Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcQuery8Result r = new LdbcQuery8Result(
                            Long.parseLong((String) doc.getAttribute("commentAuthorId")),
                            (String) doc.getAttribute("commentAuthorFirstName"),
                            (String) doc.getAttribute("commentAuthorLastName"),
                            (Long) doc.getAttribute("commentCreationDate"),
                            Long.parseLong((String) doc.getAttribute("commentId")),
                            (String) doc.getAttribute("commentContent")
                    );

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class InteractiveQuery9 implements ListOperationHandler<LdbcQuery9Result, LdbcQuery9, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery9 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery9);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery9 operation) {
            return state.getQueryStore().getQuery9Map(operation);
        }

        @Override
        public void executeOperation(LdbcQuery9 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery9Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcQuery9Result r = new LdbcQuery9Result(
                            Long.parseLong((String) doc.getAttribute("personId")),
                            (String) doc.getAttribute("personFirstName"),
                            (String) doc.getAttribute("personLastName"),
                            Long.parseLong((String) doc.getAttribute("commentOrPostId")),
                            (String) doc.getAttribute("commentOrPostContent"),
                            (Long) doc.getAttribute("commentOrPostCreationDate")
                    );

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }

    }

    /*
    public static class InteractiveQuery10 implements ListOperationHandler<LdbcQuery10Result, LdbcQuery10, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcQuery10 operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveComplexQuery10);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcQuery10 operation) {
            return state.getQueryStore().getQuery10Map(operation);
        }

        public void executeOperation(LdbcQuery10 operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcQuery10Result> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcQuery10Result r = new LdbcQuery10Result(
                            Long.parseLong((String) doc.getAttribute("personId")),
                            (String) doc.getAttribute("personFirstName"),
                            (String) doc.getAttribute("personLastName"),
                            (int) doc.getAttribute("commonInterestScore"),
                            (String) doc.getAttribute("personGender"),
                            (String) doc.getAttribute("personCityName")
                    );

                    resultList.add(r);
                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
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
    public static class ShortQuery1PersonProfile implements SingletonOperationHandler<LdbcShortQuery1PersonProfileResult, LdbcShortQuery1PersonProfile, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery1PersonProfile operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery1);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery1PersonProfile operation) {
            return state.getQueryStore().getShortQuery1PersonProfileMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                if (result.hasNext()) {
                    BaseDocument doc = result.next();
                    resultReporter.report(1,
                            new LdbcShortQuery1PersonProfileResult(
                                    (String) doc.getAttribute("firstName"),
                                    (String) doc.getAttribute("lastName"),
                                    (Long) doc.getAttribute("birthday"),
                                    (String) doc.getAttribute("locationIP"),
                                    (String) doc.getAttribute("browserUsed"),
                                    Long.decode((String) doc.getAttribute("cityId")),
                                    (String) doc.getAttribute("gender"),
                                    (Long) doc.getAttribute("creationDate")),
                            operation);
                } else {
                    resultReporter.report(0, null, operation);
                }
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class ShortQuery2PersonPosts implements ListOperationHandler<LdbcShortQuery2PersonPostsResult, LdbcShortQuery2PersonPosts, ArangoDbConnectionState> {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery2PersonPosts operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery2);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery2PersonPosts operation) {
            return state.getQueryStore().getShortQuery2PersonPostsMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcShortQuery2PersonPostsResult> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {
                    BaseDocument doc = result.next();
                    String content = (String) doc.getAttribute("messageContent");
                    if (content == null) {
                        content = (String) doc.getAttribute("messageImageFile");
                    }
//                    resultList.add(new LdbcShortQuery2PersonPostsResult(
//                            Long.valueOf((String) doc.getAttribute("messageId")),
//                            content,
//                            (Long) doc.getAttribute("messageCreationDate"),
//                            Long.valueOf((String) doc.getAttribute("originalPostId")),
//                            Long.valueOf((String) doc.getAttribute("originalPostAuthorId")),
//                            (String) doc.getAttribute("originalPostAuthorFirstName"),
//                            (String) doc.getAttribute("originalPostAuthorLastName")));

                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class ShortQuery3PersonFriends implements ListOperationHandler<LdbcShortQuery3PersonFriendsResult, LdbcShortQuery3PersonFriends, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery3PersonFriends operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery3);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery3PersonFriends operation) {
            return state.getQueryStore().getShortQuery3PersonFriendsMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcShortQuery3PersonFriendsResult> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                while (result.hasNext()) {

                    BaseDocument doc = result.next();

                    String content = (String) doc.getAttribute("messageContent");
                    if (content == null) {
                        content = (String) doc.getAttribute("messageImageFile");
                    }

                    resultList.add(new LdbcShortQuery3PersonFriendsResult(
                            Long.valueOf((String) doc.getAttribute("friendId")),
                            (String) doc.getAttribute("firstName"),
                            (String) doc.getAttribute("lastName"),
                            (Long) doc.getAttribute("friendshipCreationDate")
                    ));

                }
                resultReporter.report(1, resultList, operation);
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class ShortQuery4MessageContent implements SingletonOperationHandler<LdbcShortQuery4MessageContentResult, LdbcShortQuery4MessageContent, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery4MessageContent operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery4);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery4MessageContent operation) {
            return state.getQueryStore().getShortQuery4MessageContentMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                if (result.hasNext()) {

                    BaseDocument doc = result.next();
                    LdbcShortQuery4MessageContentResult r = new LdbcShortQuery4MessageContentResult(
                            (String) doc.getAttribute("messageContent"),
                            (Long) doc.getAttribute("messageCreationDate")
                    );

                    resultReporter.report(1, r, operation);
                }
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class ShortQuery5MessageCreator implements SingletonOperationHandler<LdbcShortQuery5MessageCreatorResult, LdbcShortQuery5MessageCreator, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery5MessageCreator operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery5);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery5MessageCreator operation) {
            return state.getQueryStore().getShortQuery5MessageCreatorMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                if (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcShortQuery5MessageCreatorResult r = new LdbcShortQuery5MessageCreatorResult(
                            Long.parseLong((String) doc.getAttribute("authorId")),
                            (String) doc.getAttribute("firstName"),
                            (String) doc.getAttribute("lastName")
                    );

                    resultReporter.report(1, r, operation);
                }
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class ShortQuery6MessageForum implements SingletonOperationHandler<LdbcShortQuery6MessageForumResult, LdbcShortQuery6MessageForum, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery6MessageForum operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery6);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery6MessageForum operation) {
            return state.getQueryStore().getShortQuery6MessageForumMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                if (result.hasNext()) {
                    BaseDocument doc = result.next();
                    LdbcShortQuery6MessageForumResult r = new LdbcShortQuery6MessageForumResult(
                            Long.parseLong((String) doc.getAttribute("forumId")),
                            (String) doc.getAttribute("forumTitle"),
                            Long.parseLong((String) doc.getAttribute("moderatorId")),
                            (String) doc.getAttribute("moderatorFirstName"),
                            (String) doc.getAttribute("moderatorLastName")
                    );

                    resultReporter.report(1, r, operation);
                }
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    public static class ShortQuery7MessageReplies implements ListOperationHandler<LdbcShortQuery7MessageRepliesResult, LdbcShortQuery7MessageReplies, ArangoDbConnectionState>
    {
        @Override
        public String getQueryString(ArangoDbConnectionState state, LdbcShortQuery7MessageReplies operation) {
            return state.getQueryStore().getParameterizedQuery(QueryType.InteractiveShortQuery7);
        }

        public Map<String, Object> getParameters(ArangoDbConnectionState state, LdbcShortQuery7MessageReplies operation) {
            return state.getQueryStore().getShortQuery7MessageRepliesMap(operation);
        }

        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation, org.ldbcouncil.snb.impls.workloads.arango.ArangoDbConnectionState state,
                                     ResultReporter resultReporter) throws DbException {
            String query = getQueryString(state, operation);
            final Map<String, Object> parameters = getParameters(state, operation);

            ArangoDatabase db = state.getDatabase();
            try {
                List<LdbcShortQuery7MessageRepliesResult> resultList = new ArrayList<>();
                final ArangoCursor<BaseDocument> result = db.query(query, parameters, null, BaseDocument.class);
                if (result.hasNext()) {
                    BaseDocument doc = result.next();
                    resultList.add(new LdbcShortQuery7MessageRepliesResult(
                            Long.parseLong((String) doc.getAttribute("commentId")),
                            (String) doc.getAttribute("commentContent"),
                            (Long) doc.getAttribute("commentCreationDate"),
                            Long.parseLong((String) doc.getAttribute("replyAuthorId")),
                            (String) doc.getAttribute("replyAuthorFirstName"),
                            (String) doc.getAttribute("replyAuthorLastName"),
                            (Boolean) doc.getAttribute("isReplyAuthorKnowsOriginalMessageAuthor")
                    ));

                    resultReporter.report(1, resultList, operation);
                }
            } catch (final ArangoDBException e) {
                throw new DbException(e);
            }
        }
    }

    // Interactive inserts

    /*
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
