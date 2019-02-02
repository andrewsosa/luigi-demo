""" Advanced Luigi example featuring SQLAlchemy, Hadoop, and the News API. """
import csv
import datetime
import json
from typing import List

import luigi
import requests
from dateutil.relativedelta import relativedelta
from luigi.contrib import hadoop, sqla
from sqlalchemy import String


class NewsTopicTask(luigi.Task):
    """ Parameters shared by news article tasks. Defining here and inheriting prevents
        declaring parameters multiple times. Keep code DRY. """

    topic: str = luigi.Parameter()
    date_to: datetime.date = luigi.DateParameter(default=datetime.date.today())
    date_from: datetime.date = luigi.DateParameter(
        default=datetime.date.today() - relativedelta(weeks=2)
    )

    @property
    def _key(self):
        return "#{}_{}_{}".format(self.topic, self.date_from, self.date_to)

    @property
    def params(self):
        return {
            "topic": self.topic,
            "date_to": self.date_to,
            "date_from": self.date_from,
        }


class DownloadArticles(NewsTopicTask):
    """ Download the articles from the News API per task parameters. """

    api_key = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"data/articles{self._key}.json")

    def run(self):
        url = (
            f"https://newsapi.org/v2/everything?"
            f"q={self.topic}"
            f"&from={self.date_from:%Y-%m-%d}"
            f"&to={self.date_to:%Y-%m-%d}"
            f"&sortBy=popularity"
            f"&apiKey={self.api_key}"
        )

        try:
            res = requests.get(url).text
            clean_text = res.replace("\\r\\n", "\\n").replace("\\n", " ")
            articles = json.loads(clean_text)["articles"]
        except:
            raise Exception(clean_text)

        with self.output().open("w") as outfile:
            json.dump(articles, outfile, indent=4)

        return articles


class LoadArticles(NewsTopicTask, sqla.CopyToTable):

    columns = [
        (["source", String(64)], {}),
        (["author", String(64)], {}),
        (["title", String(256)], {"primary_key": True}),
        (["description", String(512)], {}),
        (["url", String(256)], {}),
        (["publishedAt", String(64)], {}),
        (["content", String(512)], {}),
    ]

    connection_string = "sqlite:///data/articles.db"

    @property
    def table(self) -> str:
        return "articles" + self.requires()._key

    def requires(self) -> DownloadArticles:
        return DownloadArticles(
            topic=self.topic, date_to=self.date_to, date_from=self.date_from
        )

    def rows(self):
        with self.input().open("r") as infile:
            articles = json.load(infile)
            for article in articles:
                yield (
                    article["source"]["name"],
                    article["author"],
                    article["title"],
                    article["description"],
                    article["url"],
                    article["publishedAt"],
                    article["content"],
                )


class ParseArticles(NewsTopicTask, luigi.Task):
    """ Convert the downloaded JSON data into hadoop-compatible CSV """

    def output(self):
        return luigi.LocalTarget(f"data/articles{self._key}.csv")

    def requires(self):
        return DownloadArticles(**self.params)

    def run(self):
        with self.input().open("r") as infile:
            articles = json.load(infile)
            with self.output().open("w") as outfile:
                writer = csv.writer(outfile, delimiter=",")
                for article in articles:
                    writer.writerow(
                        [
                            article["source"]["name"],
                            article["author"],
                            article["title"],
                            article["description"],
                            article["url"],
                            article["publishedAt"],
                            article["content"],
                        ]
                    )


class CountTopicSources(NewsTopicTask, hadoop.JobTask):
    def output(self):
        return luigi.LocalTarget(f"data/sources{self._key}.tsv")

    def requires(self):
        return ParseArticles(**self.params)

    def mapper(self, row):
        source = row.split(",")[0]
        yield source, 1

    def reducer(self, key, values):
        yield key, sum(values)


class CountMultiTopicSources(hadoop.JobTask):
    month: datetime.date = luigi.MonthParameter()
    topics: List[str] = luigi.ListParameter(
        default=["Apple", "Microsoft", "Google", "Amazon", "Facebook"]
    )

    def output(self):
        return luigi.LocalTarget(
            f"data/sources#{'_'.join(self.topics)}_{self.month}.tsv"
        )

    def requires(self):
        return [
            CountTopicSources(
                topic=topic,
                date_from=self.month,
                date_to=self.month + relativedelta(months=1),
            ) for topic in self.topics
        ]

    def mapper(self, row):
        source, count = row.strip().split("\t")
        yield source, int(count)

    def reducer(self, key, values):
        yield key, sum(values)
