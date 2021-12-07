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

## Future work
* Move zapier post job over to this pipeline with gravity forms webhooks
* Add interactive features to visualization, hosted somewhere
    - user defined windows of time.   
    - allow users to filter or color-code by any one of the following (Forecast zone, avalanche type, failure interface, trigger, size)  
    - interactive visualization i.e. clickable points that give a little info and maybe a link to the actual observation and/or a hover feature that draws out the most basic information (Trigger -Avalanche Type- Rsize -Dsize - Failure interface) in a box or something. 
    - instead of all the points being on the same color scheme linked to time they would be colored by the forecast that day e.g. avalanches that happened in a low danger zone/day would be green - maybe still fading out with time.  Or two XY plots stacked on top of each other, one with the danger rating for the day (for all 3 elevation bands) and the next plot shows avalanche activity (weighted by size) in stacked bars. 
    - Displaying avalanche activity on a map with a time slider (We could provide coordinates for each of our location fields, it wouldn't be perfect, but approximate)
* Re-map the integer fields on the form to be numeric instead of string to avoid things like '900-1000'. Not sure how the API handles changes to the forms.
* Some duplicative code in observation_endpoints.sql and avy_long_format. (low priority)
* Set up CI/CD so that google cloud function pulls from master copy of main.py instead of sending it up with the gcloud deploy function. (low priority)