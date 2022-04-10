SELECT
 obs.* 
FROM `cbac-306316.cbac_wordpress.obs_form_12_direct` obs
LEFT JOIN `cbac-306316.cbac_wordpress.wp_posts_view` v ON obs.id = v.entry_id
WHERE v.entry_id IS NULL
ORDER BY 1 DESC