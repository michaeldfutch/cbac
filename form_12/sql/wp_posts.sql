--DROP TABLE IF EXISTS `cbac-306316.cbac_wordpress.wp_posts_raw`;
CREATE TABLE `cbac-306316.cbac_wordpress.wp_posts_raw` (
title	string,
url string,
date string,
content string

);

drop view if exists  `cbac-306316.cbac_wordpress.wp_posts_view`;
create view `cbac-306316.cbac_wordpress.wp_posts_view` as (
select 
DISTINCT
trim(reverse(substring(reverse(content),6, strpos(reverse(content),'>vid/<')-6))) entry_id,
url
 from cbac-306316.cbac_wordpress.wp_posts_raw_v2
 where strpos(reverse(content),'>vid/<') = 11
);

