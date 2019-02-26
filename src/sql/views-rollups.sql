-- Create "album posts" that combine an albums' title and description with
-- its constituent images' descriptions.
create temp view album_posts as
select
    a.id,
    a.media_id,
    case
        when a.body = '' then i.post
        else a.body || '\n' || i.post
    end as post
from (
    -- Format album text.
    select
        id,
        media_id,
        trim(coalesce(title, '') || '\n' || coalesce(description, ''), '\n') as body
    from albums
) as a
inner join (
    -- Roll-up image descriptions.
    select
        media_id,
        -- Skip image titles, as there are only a few and they
        -- don't add much when they are there.
        trim(group_concat(description, '\n'), '\n') as post
    from images
    where description is not null
    group by media_id
    having group_concat(description, '\n') <> ''
) as i
on a.media_id = i.media_id;


-- All rolled-up medias.
-- Assume that images that are not part of albums do not
-- have meaningful titles or descriptions, and as such
-- are left out.
create temp view media_rollups as
select
    m.submission_id,
    m.id as media_id,
    a.uploaded_utc as album_uploaded,
    a.views as album_views,
    i.n_images,
    i.has_album,
    i.image_views,
    i.first_uploaded as first_image_uploaded,
    i.last_uploaded as last_image_uploaded,
    album_posts.post
from medias as m
left outer join albums as a
on m.id = a.media_id
left outer join (
    -- Image-level data (non-text).
    select
        media_id,
        count(id) as n_images,
        count(album_id) > 0 as has_album,
        sum(views) as image_views,
        min(uploaded_utc) as first_uploaded,
        max(uploaded_utc) as last_uploaded
    from images
    group by media_id
) as i
on m.id = i.media_id
left outer join album_posts
on m.id = album_posts.media_id;


-- Roll-up to the submission level.
-- Things get a little hairy, as we generally expect
-- one album per submission, but anyways.
create temp view rollups as
select
    s.id as submission_id,
    s.title as submission_title,
    s.author,
    s.created_utc as submitted_timestamp,
    s.selftext_html,
    s.comments,
    s.gilded,
    s.downs,
    s.ups,
    m.*
from submissions as s
left outer join (
    select
        submission_id,
        sum(has_album) as n_albums,
        min(album_uploaded) as first_album_uploaded,
        max(album_uploaded) as last_album_uploaded,
        sum(album_views) as total_album_views,
        sum(n_images) as total_images,
        first_image_uploaded,
        last_image_uploaded,
        trim(group_concat(post, '\n\n'), '\n') as posts
    from media_rollups
    group by submission_id
) as m
on s.id = m.submission_id;
