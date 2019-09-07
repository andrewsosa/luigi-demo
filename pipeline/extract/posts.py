import luigi
from luigi import contrib
from pymongo import MongoClient
from .. import config


class ExtractMongoPosts(luigi.Task):
    posts_from = luigi.DateParameter()
    client = MongoClient(config.MONGO_URI)

    def output(self):
        return luigi.LocalTarget("data/posts%s.tsv" % self.date_interval)

    def run(self):
        db = self.client['snews']
        reddit_posts = db['reddit-posts']

