# CBAC Observation Pipeline and Visualization

## form_11
[Deprecated]

## form_12

Steps in the CBAC data flow:
1. User submits and observation on the wordpress site
2. A forecaster approves the observation and a post is generated, with a key at the bottom of
of the content indicating the form entry id.
3. There is a zapier function that listens for all new posts and sends them to a bigquery table called 
`cbac_wordpress.wp_posts_raw` (TODO: move this off zapier). 
4. A scheduled google cloud function runs the `main.py` script at 4:05am everyday. This step may happen before or after step 2.
    * Read all form 12 submissions via API call to gravity forms
    * Drop/rebuild table `cbac_wordpress.obs_form_12_direct`
    * Reformat raw form entries into a format useable by CAIC using `sql/observation_endpoint.sql`. To keep only 
    approved posts from going through, join to `cbac_wordpress.wp_posts_view` which contains the form entry ids for
    all approved posts, scraped from the key from step 2 (see `wp_posts.sql`)
    * Export tables to google sheets for public access to avalanche data
    * Build visualization with matplotlib, static image saved in google cloud storage bucket every day.
