"""Plotly figures for member 4; accept snapshot data, never open the live DB."""
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def current_state_figure(station):
    fig = make_subplots(rows=1, cols=2, specs=[[{'type':'indicator'},{'type':'indicator'}]])
    fig.add_trace(go.Indicator(mode='number', value=station['active_sessions'],
                  title={'text':'Active sessions'}), row=1, col=1)
    price = station['price_per_kwh']
    if price is not None:
        fig.add_trace(go.Indicator(mode='number', value=price,
                      number={'prefix':'A$','valueformat':'.3f'},
                      title={'text':'Latest received price / kWh'}), row=1, col=2)
    else:
        fig.add_annotation(text='Price not received', x=.8,y=.5,xref='paper',yref='paper',showarrow=False)
    fig.update_layout(title=station['display_name'],template='plotly_white',height=280)
    return fig


def history_figure(daily_rows):
    fig = make_subplots(specs=[[{'secondary_y':True}]])
    dates = [str(r['completion_date']) for r in daily_rows]
    fig.add_trace(go.Bar(x=dates,y=[r['energy_kwh'] for r in daily_rows],name='Completed energy (kWh)'),secondary_y=False)
    fig.add_trace(go.Scatter(x=dates,y=[r['completed_sessions'] for r in daily_rows],
                  name='Completed sessions',mode='lines+markers'),secondary_y=True)
    fig.update_yaxes(title_text='Energy (kWh)',secondary_y=False)
    fig.update_yaxes(title_text='Completed sessions',secondary_y=True,rangemode='tozero',dtick=1)
    fig.update_xaxes(title_text='Completion date (Australia/Sydney)',type='category')
    fig.update_layout(title='History from received events only',template='plotly_white',height=380)
    if not daily_rows:
        fig.add_annotation(text='No completed sessions received',x=.5,y=.5,xref='paper',yref='paper',showarrow=False)
    return fig


def live_curve_figure(session):
    points = session['meter_curve']
    fig=make_subplots(specs=[[{'secondary_y':True}]])
    x=[str(p['event_ts']) for p in points]
    fig.add_trace(go.Scatter(x=x,y=[p['power_kw'] for p in points],name='Power (kW)',mode='lines+markers'),secondary_y=False)
    fig.add_trace(go.Scatter(x=x,y=[p['cumulative_energy_kwh'] for p in points],name='Cumulative energy (kWh)',mode='lines'),secondary_y=True)
    fig.update_yaxes(title_text='Power (kW)',secondary_y=False)
    fig.update_yaxes(title_text='Cumulative energy (kWh)',secondary_y=True)
    fig.update_xaxes(title_text='Received event time (UTC)')
    fig.update_layout(title=f"Session {session['session_id']} — {session['status']}",template='plotly_white',height=380)
    return fig
