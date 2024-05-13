#Channel Table:
create table if not exists channel(
                                    channel_id 			varchar(80) primary key,
                                    channel_name		varchar(100),
                                    subscribers	        bigint,
                                    channel_views		bigint,
                                    total_videos	    integer,
                                    channel_description	text,
                                    playlist_id			varchar(80));

#Playlist Table:
create table if not exists playlist(
                                    playlist_id		varchar(80) primary key,
                                    title	        varchar(100),
                                    channel_id		varchar(80),
                                    channel_name	varchar(100),
                                    publishedat		timestamp,
                                    total_videos    bigint);

#Videos Table:
create table if not exists videos(
                                    video_id			varchar(100) primary key,
                                    channel_id          varchar(80),
                                    title	        	varchar(150),
                                    channel_name        varchar(100),
                                    description	        text,
                                    publishedat			timestamp,
                                    tags				text,
                                    thumbnail			varchar(200),
                                    duration			INTERVAL,
                                    caption     		varchar(100),
                                    definition          varchar(10),
                                    view_count			bigint,
                                    like_count			bigint,
                                    comment_count		bigint,
                                    fav_count		    bigint);

#Comments Table:
create table if not exists comments_tbl(
                                    comment_id				varchar(100) primary key,
                                    video_id				varchar(100),
                                    comment_text			varchar(100000),
                                    comment_author			varchar(100),
                                    publishedat	            timestamp
                                    );
