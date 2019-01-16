import csv
import datetime
import luigi
import pandas
import random


from .simple import (
    AggregatePositions,
    ProfessionEmployers,
    ScientistEmployers,
    GenerateCustomers,
)


class CompanyEngineerSalary(luigi.Task):
    """ Dynamically queues ProfessionEmployers tasks based on AggregatePosition results. """

    month: datetime.date = luigi.MonthParameter(default=datetime.date.today())
    job: str = luigi.Parameter(default='engineer')

    def output(self):
        return luigi.LocalTarget(f"data/company_position_salary_{self.month}.csv")

    def requires(self):
        return AggregatePositions(month=self.month)

    def run(self):

        with self.input().open() as positions_file:
            positions = positions_file.readlines()

        with self.output().open("w") as outfile:
            writer = csv.writer(outfile)

            # Iterate over customer jobs
            for job, num in [p.split("\t") for p in positions]:
                if self.job.lower() in job.lower():
                    # Queue job to find `job`'s employers
                    target = yield ProfessionEmployers(job=job)

                    # Read a list of all employers
                    employers = [
                        line.split("\t")[0] for line in target.open().readlines()
                    ]

                    # "Calculate" the salary for each position at each company
                    for emplr in employers:
                        writer.writerow([job, emplr, random.randint(50, 150) * 1000])


class PandasDFDemo(luigi.Task):
    """ Print a sample dataframe """

    month: datetime.date = luigi.MonthParameter(default=datetime.date.today())
    profession: str = luigi.Parameter(default="Engineer")

    def requires(self):
        return GenerateCustomers(month=self.month)

    def output(self):
        return luigi.LocalTarget(f"data/customers#{self.profession}_{self.month}.txt")

    def run(self):
        with self.input().open() as customer_file:
            customer_df = pandas.read_csv(
                customer_file,
                names=["Name", "Address", "Birthdate", "Job", "Company", "Email"],
            )

        with self.output().open("w") as outfile:
            outfile.write(str(customer_df))

        return customer_df


class CustomerSalaries(luigi.Task):
    """ Load the customers into a dataframe """

    month: datetime.date = luigi.MonthParameter(default=datetime.date.today())
    job: str = luigi.Parameter(default="Engineer")

    def output(self):
        return luigi.LocalTarget(f"data/salaries_{self.job}_{self.month}.csv")

    def requires(self):
        return {
            "a": GenerateCustomers(month=self.month),
            "b": CompanyEngineerSalary(month=self.month, job=self.job),
        }

    def run(self):
        with self.input()["a"].open() as customer_file:
            customer_df = pandas.read_csv(
                customer_file,
                names=["Name", "Address", "Birthdate", "Job", "Company", "Email"],
            )

        with self.input()["b"].open() as salaries_file:
            salaries_df = pandas.read_csv(
                salaries_file, delimiter=",", names=["Position", "Company", "Salary"]
            )

        employees_df = customer_df[['Name', 'Company']]
        employee_salaries_df = employees_df.merge(salaries_df, on="Company")
        employee_salaries_df.to_csv(self.output().path)
        print(employee_salaries_df.head())


