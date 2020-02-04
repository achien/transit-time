# transit-time

Code for http://transit.andrewchien.com/timelapse/

![](https://raw.githubusercontent.com/achien/transit-time/master/.github/timelapse.gif)

## Deployment

All data lives in a Postgres database. To run:
* `scraper/scraper.py` downloads all MTA data feeds. Runs every minute (can be more/less frequent; MTA refreshes feed every 30 seconds).
* `scraper/batch_process.py` takes the downloaded data and determines where trains are. Repeated runs with `--job-name` will pick up from where the last run left off. Runs every minute. Can be used to backfill data as well.
* `uvicorn web.app:app` to start the web server

To run the code, environment variables need to be set.
* `POSTGRES_HOST`
* `POSTGRES_USER`
* `POSTGRES_PASSWORD`
* `NYC_MTA_API_KEY` is only required for scraper.py
