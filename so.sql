create table badges (
    Id INT NOT NULL PRIMARY KEY,
    UserId INT,
    Name VARCHAR,
    Date TIMESTAMP,
    Class INT,
    TagBased INT
);

CREATE TABLE comments (
    Id INT NOT NULL PRIMARY KEY,
    PostId INT NOT NULL,
    Score INT NOT NULL DEFAULT 0,
    Text TEXT,
    CreationDate TIMESTAMP,
    UserId INT NOT NULL
);

CREATE TABLE post_history (
    Id INT NOT NULL PRIMARY KEY,
    PostHistoryTypeId SMALLINT NOT NULL,
    PostId INT NOT NULL,
    RevisionGUID VARCHAR,
    CreationDate TIMESTAMP,
    UserId INT NOT NULL,
    Text TEXT
);

CREATE TABLE posts (
    Id INT NOT NULL PRIMARY KEY,
    PostTypeId SMALLINT,
    AcceptedAnswerId INT,
    ParentId INT,
    Score INT NULL,
    ViewCount INT NULL,
    Body text NULL,
    OwnerUserId INT NOT NULL,
    LastEditorUserId INT,
    LastEditDate TIMESTAMP,
    LastActivityDate TIMESTAMP,
    Title varchar NOT NULL,
    Tags VARCHAR,
    AnswerCount INT NOT NULL DEFAULT 0,
    CommentCount INT NOT NULL DEFAULT 0,
    FavoriteCount INT NOT NULL DEFAULT 0,
    CreationDate TIMESTAMP,
    ContentLicense VARCHAR
);

CREATE TABLE users (
    Id INT NOT NULL PRIMARY KEY,
    Reputation INT NOT NULL,
    CreationDate TIMESTAMP,
    DisplayName VARCHAR NULL,
    LastAccessDate TIMESTAMP,
    Views INT DEFAULT 0,
    WebsiteUrl VARCHAR NULL,
    Location VARCHAR NULL,
    AboutMe TEXT NULL,
    Age INT,
    UpVotes INT,
    DownVotes INT,
    EmailHash VARCHAR
);

CREATE TABLE votes (
    Id INT NOT NULL PRIMARY KEY,
    PostId INT NOT NULL,
    VoteTypeId SMALLINT,
    CreationDate TIMESTAMP
);

CREATE TABLE post_links (
    Id INT NOT NULL PRIMARY KEY,
    CreationDate TIMESTAMP,
    PostId INT,
    RelatedPostId INT,
    LinkTypeId INT
);

CREATE TABLE tags (
    Id INT,
    TagName VARCHAR,
    Count VARCHAR,
    ExcerptPostId VARCHAR,
	WikiPostId VARCHAR,
);

create index badges_idx_1 on badges(UserId);

create index comments_idx_1 on comments(PostId);

create index comments_idx_2 on comments(UserId);

create index post_history_idx_1 on post_history(PostId);

create index post_history_idx_2 on post_history(UserId);

create index posts_idx_1 on posts(AcceptedAnswerId);

create index posts_idx_2 on posts(ParentId);

create index posts_idx_3 on posts(OwnerUserId);

create index posts_idx_4 on posts(LastEditorUserId);

create index votes_idx_1 on votes(PostId);