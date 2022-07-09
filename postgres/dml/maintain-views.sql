INSERT INTO Message
    SELECT
        creationDate,
        id AS id,
        language,
        content,
        imageFile,
        locationIP,
        browserUsed,
        length,
        CreatorPersonId,
        ContainerForumId,
        LocationCountryId,
        NULL::bigint AS ParentMessageId
    FROM Post
;

INSERT INTO Message
    SELECT
        Comment.creationDate AS creationDate,
        Comment.id AS id,
        NULL,
        Comment.content AS content,
        NULL AS imageFile,
        Comment.locationIP AS locationIP,
        Comment.browserUsed AS browserUsed,
        Comment.length AS length,
        Comment.CreatorPersonId AS CreatorPersonId,
        NULL AS ContainerForumId,
        Comment.LocationCountryId AS LocationCityId,
        coalesce(Comment.ParentPostId, Comment.ParentCommentId) AS ParentMessageId
    FROM Comment
;

INSERT INTO Person_likes_Message
    SELECT creationDate, PersonId, CommentId AS id FROM Person_likes_Comment
    UNION ALL
    SELECT creationDate, PersonId, PostId AS id FROM Person_likes_Post
;

INSERT INTO Message_hasTag_Tag
    SELECT creationDate, CommentId AS id, TagId FROM Comment_hasTag_Tag
    UNION ALL
    SELECT creationDate, PostId AS id, TagId FROM Post_hasTag_Tag
;
