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

create table albums (
    id varchar primary key,
    media_id integer not null,
    title varchar,
    description varchar,
    uploaded_utc integer not null,
    url varchar not null,
    views integer not null,

    date_created datetime default current_timestamp,
    foreign key (media_id) references medias(id)
);

create table images (
    id varchar primary key,
    media_id integer not null,
    album_id varchar,
    title varchar,
    description varchar,
    uploaded_utc integer,
    mimetype varchar,
    url varchar not null,
    views integer,
    img blob,

    date_created datetime default current_timestamp,
    foreign key (media_id) references medias(id),
    foreign key (album_id) references albums(id)
);

/* Zappos. */
create table searches (
    brand varchar not null,
    product_id integer not null,
    product_name varchar not null,
    category varchar not null,

    search_query varchar not null,
    date_created datetime default current_timestamp
);
create index product_id_idx on searches(product_id);

create table products (
    id integer primary key,
    brand varchar not null,
    name varchar not null,
    default_url varchar not null,
    description varchar,

    date_created datetime default current_timestamp
);
