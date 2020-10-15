import context, task, es, closing
from pprint import pprint


class Runner(context.Context, task.Task, es.Elastic_search, closing.Closing):

    # Calling Context
    def context_run(self):
        spark = self.context()
        print("Spark Context Created !!")
        return spark

    # Creating Spark DataFrame
    def data_loading(self, spark):
        df = self.load_data(spark)
        df1 = df["Emp ID", "Month Name of Joining", "Gender", "County", "State", "Region",
                 "City", "Salary", "Last % Hike"]
        return df1

    # Write data into Elasticsearch
    def write_data(self, df, index):
        return self.write_data_onto_elasticsearch(df, index)

    # Establish Elasticsearch Connection
    def establish_conn(self):
        print("Elasticsearch Connection Established !!")
        return self.establish_connection()

    # Closing Spark Context
    def sp_close(self, spark):
        return self.spark_closing(spark)

    # Closing Elasticsearch Connection
    def es_close(self, elastic):
        return self.elastic_closing(elastic)

    # Calling all the task executing based on task-Number Input
    def run(self, con, index_name='name'):
        while True:
            var = input('Enter for Processing: ')
            if var == 'task-1':
                try:
                    pprint(self.region(con, index_name))
                    pprint(self.city(con, index_name))
                    pprint(self.country(con, index_name))
                except:
                    print("Task-1 didn't Executed")
            elif var == 'task-2':
                try:
                    pprint(self.summary(con, index_name))
                except:
                    print("Task-2 didn't Executed")
            elif var == 'task-3':
                try:
                    pprint(self.orderby_gender_salary(con, index_name))
                except:
                    print("Task-3 didn't Executed")
            elif var == 'task-4':
                try:
                    pprint(self.employee_joined_based_on_month(con, index_name))
                    pprint(self.hikes_granted_based_on_month(con, index_name))
                except:
                    print("Task-4 didn't executed")
            elif var == 'task-5':
                try:
                    pprint(self.sort_salary(con, index_name))
                except:
                    print("Task-5 didn't Executed")
            elif var == 'done':
                break
            else:
                print("Wrong Input!!")
                continue


if __name__ == '__main__':
    runner = Runner()
    ind = input("Enter Index name: ")
    print(runner.check_connection())
    try:
        elastic_con = runner.establish_conn()
    except:
        print("Elasticsearch Connection not Established")
    try:
        sp = runner.context_run()
    except:
        print("Spark Context didn't get Created !!")
    df = runner.data_loading(sp)
    runner.write_data(df, ind).save()
    runner.run(elastic_con, ind)
    try:
        runner.es_close(elastic_con)
        print('Elasticsearch Connection Closed !!')
        runner.sp_close(sp)
        print('Spark Context Closed !!')
    except:
        print("Connection didn't get Closed")
