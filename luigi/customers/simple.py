""" Simple Luigi demo for customers, professions, and companies. """

import csv
import datetime
import luigi
import faker

from luigi.contrib import hadoop


class GenerateCustomers(luigi.Task):
    """ Generate :count:-many customers from :month:. """

    month: datetime.date = luigi.MonthParameter()
    count: int = luigi.IntParameter(default=10000)

    def output(self):
        return luigi.LocalTarget(f"data/customers_{self.month}.csv")

    def run(self):
        fake = faker.Faker()

        with self.output().open("w") as outfile:
            writer = csv.writer(outfile)
            for _ in range(self.count):
                writer.writerow(
                    [
                        fake.name(),
                        fake.address().replace("\n", " "),
                        fake.date_between_dates(
                            date_start=datetime.date(1930, 1, 1),
                            date_end=datetime.date(2000, 1, 1),
                        ),
                        fake.job(),
                        fake.company(),
                        fake.company_email(),
                    ]
                )


class AggregatePositions(hadoop.JobTask):
    """ Sum the number of each position for all customers. """

    month: datetime.date = luigi.MonthParameter()

    def output(self):
        return luigi.LocalTarget(f"data/customer_positions_{self.month}.tsv")

    def requires(self):
        return GenerateCustomers(month=self.month)

    def mapper(self, row):
        _reader = csv.reader([row])
        cols = list(_reader)[0]
        yield cols[3], 1

    def reducer(self, key, values):
        yield key, sum(values)


class ProfessionEmployers(hadoop.JobTask):
    """ Count customer positions """

    month: datetime.date = luigi.MonthParameter(default=datetime.date.today())
    job: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"data/employers#{self.job}_{self.month}.tsv")

    def requires(self):
        return GenerateCustomers(month=self.month)

    def mapper(self, row):
        _reader = csv.reader([row])
        cols = list(_reader)[0]
        position, company = cols[3], cols[4]

        if self.job.lower() in position.lower():
            yield company, 1

    def reducer(self, key, values):
        yield key, sum(values)


class ScientistEmployers(hadoop.JobTask):
    """ Aggregate the complete list of all scientists and engineer employers """

    month: datetime.date = luigi.MonthParameter(default=datetime.date.today())
    jobs: list = luigi.ListParameter(
        default=["scientist", "engineer"]
    )

    def output(self):
        return luigi.LocalTarget(f"data/employers#{'_'.join(self.jobs)}_{self.month}.tsv")

    def requires(self):
        return [ProfessionEmployers(job=job) for job in self.jobs]

    def mapper(self, row):
        company, employees = row.split('\t')
        yield company, int(employees)

    def reducer(self, key, values):
        yield key, sum(values)
