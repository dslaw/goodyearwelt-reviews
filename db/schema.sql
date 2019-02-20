create table submissions (
    id varchar primary key,
    title varchar not null,
    author_fullname varchar not null,
    author varchar not null,
    subreddit varchar not null,
    permalink varchar not null unique,
    created_utc integer not null,

    selftext_html varchar,
    comments integer not null,
    gilded integer not null,
    downs integer not null,
    ups integer not null,
    score integer not null,

    search_query varchar not null,
    date_created datetime default current_timestamp
);

create table medias (
    id integer primary key autoincrement,
    submission_id varchar not null,
    url varchar not null,
    is_direct boolean not null,
    txt varchar,
    foreign key (submission_id) references submissions(id)
);
