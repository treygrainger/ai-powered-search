import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

def plot(client):
    init_notebook_mode(connected=True)

    models = ['classic', 'latest']
    modelData = []

    if client.name() == 'elastic':
        query = {
            "bool": {
                "must": {"match_all": {} },
                "filter": {
                    "match": {"title": "batman"}
                    }
            }
        }
    else:
        query = 'title:(batman)^0'

    for model in models:
        modelData.append(client.model_query('tmdb', model, {}, query))

    xAxes = []
    for i in range(len(modelData[0])):
        xAxes.append(i)

    trace0 = go.Scatter(
        x = xAxes,
        y = [x['release_year'] for x in modelData[0]],
        mode = "lines",
        name = "classic"
    )

    trace1 = go.Scatter(
        x = xAxes,
        y = [x['release_year'] for x in modelData[1]],
        mode = "lines",
        name = "latest"
    )


    data = [trace0, trace1]
    fig = go.Figure(data=data)
    iplot(fig)
