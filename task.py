class Task:

    # Count Number of Employees in each Region
    def region(self, es, name='name'):
        return es.search(index=name, size=0, body={"query": {"match_all": {}}, "aggs": {
            "No. of Employees per Region": {"terms": {"field": "Region.keyword"}}}})

    # Count Number of Employees in each Country
    def country(self, es, name='name'):
        return es.search(index=name, size=0, body={"query": {"match_all": {}}, "aggs": {
            "No. of Employees in per County": {"terms": {"field": "County.keyword"}}}})

    # Count Number of Employees in each City
    def city(self, es, name='name'):
        return es.search(index=name, size=0, body={"query": {"match_all": {}}, "aggs": {
            "No. of Employees per City": {"terms": {"field": "City.keyword"}}}})

    # Generate Employee Summary
    def summary(self, es, name='name'):
        return es.search(index=name, size=5, body={"query": {"match_all": {}}})

    # Orderby Gender and Salary
    def orderby_gender_salary(self, es, name='name'):
        return es.search(index=name, size=3, body={"query": {"match_all": {}},
                                                   "sort": [{"Gender.keyword": {"order": "asc"}},
                                                            {"Salary": {"order": "asc"}}]})

    # Hikes Granted based on Month
    def hikes_granted_based_on_month(self, es, name='name'):
        return es.search(index=name, size=0, body={"query": {"match_all": {}}, "aggs": {
            "No. of Hikes granted per Month": {"cardinality": {"field": "Salary Hike"}}}})

    # Employee joined based on Month
    def employee_joined_based_on_month(self, es, name='name'):
        return es.search(index=name, size=0, body={"query": {"match_all": {}}, "aggs": {
            "No. of Employees joined in particular month": {"terms": {"field": "Month Name of Joining.keyword"}}}})

    # Salary Wise sort Employee Data
    def sort_salary(self, es, name='name'):
        return es.search(index=name, size=5,
                         body={"query": {"match_all": {}}, "sort": [{"Salary": {"order": "asc"}}]})
