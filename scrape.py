import schedule, time, datetime, os, sys, logging, json, rethinkdb as r
import scrape_projects_recent, scrape_projects_live

LOGGING = {
    'stream': sys.stdout,
    'level': logging.INFO,
    'format': '%(asctime)s %(levelname)s %(message)s',
}
DB = {
    'host': 'rethinkdb',
    'port': 28015,
}
DATABASE = 'kickstarter'
TABLES = [
    {
        'name': 'log',
    },
    {
        'name': 'projects_recently_launched',
        'indexes': ['launched_at', 'deadline'],
    },
    {
        'name': 'projects_recently_funded',
        'indexes': ['launched_at', 'deadline'],
    },
    {
        'name': 'projects_live',
        'primary': 'scraped',
        'indexes': ['id', 'launched_at', 'deadline'],
    },
]

def logStamp():
    return {
        'iso': datetime.datetime.utcnow().isoformat(),
        'epoch': int(time.time()),
    }

def logScrape(log, f):
    try:
        f()
        log['done'] = logStamp()
    except Exception as e:
        log['fail'] = logStamp()
    finally:
        logging.info(json.dumps(log))
        r.table('log').insert(log).run()

def logRecentScrape(filter, minutes):
    log = {
        'filter': filter,
        'minutes': minutes,
        'start': logStamp(),
        'type': 'recent',
    }
    logScrape(log, lambda: scrape_projects_recent.scrape(filter, minutes))

def scrapeLive():
    log = {
        'start': logStamp(),
        'type': 'live',
    }
    logScrape(log, lambda: scrape_projects_live.scrape())

logging.basicConfig(**LOGGING)
logging.getLogger('requests').setLevel(logging.WARNING)

logging.info('Started preparing')
connection = r.connect(**DB).repl()
connection.repl()
if DATABASE not in r.db_list().run():
    logging.info('Creating database %s' % DATABASE)
    r.db_create(DATABASE).run()
connection.use(DATABASE)
for table in TABLES:
    if table['name'] not in r.table_list().run():
        logging.info('Creating table %s' % table['name'])
        if 'primary' in table:
            r.table_create(table['name'], primary_key=table['primary']).run()
        else:
            r.table_create(table['name']).run()
    if not 'indexes' in table: continue
    indexes = set(r.table(table['name']).index_list().run())
    indexes = set(table['indexes']) - indexes
    for index in indexes:
        logging.info('Creating index %s on %s' % (index, table['name']))
        r.table(table['name']).index_create(index).run()
r.wait()
logging.info('Finished preparing')

# https://github.com/dbader/schedule/issues/55
logging.info('Started scheduling jobs')
schedule.every().hour.at('00:00').do(lambda: logRecentScrape('launched', 65))
schedule.every().hour.at('00:05').do(lambda: logRecentScrape('funded', 65))
schedule.every().hour.at('00:10').do(scrapeLive)
if os.environ.get('PROD') is None:
    logging.info('Running all jobs and exiting')
    schedule.run_all(0)
    sys.exit(0)
logging.info('Finished scheduling jobs')

while True:
    schedule.run_pending()
    time.sleep(1)
