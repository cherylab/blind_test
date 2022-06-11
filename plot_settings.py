import plotly.graph_objs as go

background_color = "#ffffff"
grid_color = "#f2f0f0"
color_list = ['#5C943D', "#bababa"]
color_list = ["#2782C0",'#868686',"#D8A360",'#cd534c','#7aa6dc','#003c66']

dockstreet_template = go.layout.Template(
    layout=go.Layout(
        colorway=color_list,
        font={"color": "black", "family": "sans-serif"},
        mapbox={"style": "light"},
        paper_bgcolor=background_color,
        plot_bgcolor=background_color,
        hovermode="closest",
        xaxis={
            "automargin": True,
            "gridcolor": grid_color,
            "linecolor": grid_color,
            "ticks": "",
            "zerolinecolor": grid_color,
            "zerolinewidth": 2,
        },
        yaxis={
            "automargin": True,
            "gridcolor": grid_color,
            "linecolor": grid_color,
            "ticks": "",
            "zerolinecolor": grid_color,
            "zerolinewidth": 2,
            "title_standoff": 10,
        },
    )
)

# ---------------------------------------
background_color_dash = "#fbfcfc"
grid_color_dash = "#f1f1f1"
color_list_dash = ['#5C943D', "#afafaf"]

dockstreet_template_dash = go.layout.Template(
    layout=go.Layout(
        colorway=color_list_dash,
        font={"color": "black", "family": "sans-serif"},
        mapbox={"style": "light"},
        paper_bgcolor=background_color_dash,
        plot_bgcolor=background_color_dash,
        hovermode="closest",
        xaxis={
            "automargin": True,
            "gridcolor": grid_color_dash,
            "linecolor": grid_color_dash,
            "ticks": "",
            "zerolinecolor": grid_color_dash,
            "zerolinewidth": 2,
        },
        yaxis={
            "automargin": True,
            "gridcolor": grid_color_dash,
            "linecolor": grid_color_dash,
            "ticks": "",
            "zerolinecolor": grid_color_dash,
            "zerolinewidth": 2,
            "title_standoff": 10,
        },
    )
)