CREATE TABLE votes (
    Id INT NOT NULL PRIMARY KEY,
    PostId INT,
    VoteTypeId SMALLINT,
    CreationDate TIMESTAMP
);

CREATE TABLE tags (
    Id INT,
    TagName VARCHAR,
    Count INT,
    ExcerptPostId VARCHAR,
	WikiPostId VARCHAR
);

CREATE TABLE posts (
    Id INT NOT NULL PRIMARY KEY,
    PostTypeId SMALLINT,
    AcceptedAnswerId INT,
    ParentId INT,
    Score INT NULL,
    ViewCount INT NULL,
    Body text NULL,
    OwnerUserId INT,
    LastEditorUserId INT,
    LastEditDate TIMESTAMP,
    ClosedDate TIMESTAMP,
    LastActivityDate TIMESTAMP,
    Title varchar,
    Tags VARCHAR,
    AnswerCount INT DEFAULT 0,
    CommentCount INT DEFAULT 0,
    FavoriteCount INT DEFAULT 0,
    CreationDate TIMESTAMP,
    CommunityOwnedDate TIMESTAMP,
    ContentLicense VARCHAR,
	LastEditorDisplayName VARCHAR,
	OwnerDisplayName VARCHAR
);

CREATE TABLE postLinks (
    Id INT NOT NULL PRIMARY KEY,
    CreationDate TIMESTAMP,
    PostId INT,
	RelatedPostId INT,
	LinkTypeId INT
);

CREATE TABLE comments (
    Id INT NOT NULL PRIMARY KEY,
    PostId INT,
    Score INT DEFAULT 0,
    Text TEXT,
    CreationDate TIMESTAMP,
    UserId INT,
	ContentLicense VARCHAR,
	UserDisplayName VARCHAR
);

create table badges (
    Id INT NOT NULL PRIMARY KEY,
    UserId INT,
    Name VARCHAR,
    Date TIMESTAMP,
    Class INT,
    TagBased INT
);


CREATE TABLE users (
    Id INT NOT NULL PRIMARY KEY,
    Reputation INT,
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
    EmailHash VARCHAR,
	AccountId INT,
	ProfileImageUrl VARCHAR
);
