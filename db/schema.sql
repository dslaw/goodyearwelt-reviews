create table submission_facts (
    id char(6) primary key,
    title varchar not null,
    author_fullname varchar not null,
    url varchar not null unique,
    created_utc integer not null,
    search_query varchar not null,
    date_created datetime default current_timestamp
);

create table submissions (
    id char(6) primary key,
    selftext_html text,
    comments integer not null,
    gilded integer not null,
    downs integer not null,
    ups integer not null,
    score integer not null,
    foreign key (id) references submission_facts(id)
);
