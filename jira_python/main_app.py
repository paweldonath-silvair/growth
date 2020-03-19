import plotly.graph_objects as go
from plotly.offline import plot

from data_utils import IssueDetails
from jira_api import get_issue, search_issues
from figures_utils import generate_table


issues = search_issues(sprint='Delta 2020 week 13')
table = generate_table(issues)
layout = go.Layout()
figure = go.Figure(data=[table], layout=layout)
plot(figure, auto_open=True, filename='test.html')
