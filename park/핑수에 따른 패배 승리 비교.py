
#핑수에따른 패배,승리 바이올린 비교
d=df[['CHAMPIONNAME','WIN','ENEMYMISSINGPINGS']]

plotly.offline.init_notebook_mode(connected=True)

fig = px.violin(d, y="ENEMYMISSINGPINGS", color="WIN",
                violinmode='overlay',
                hover_data=d.columns)
fig.show()